import asyncio
import datetime
import logging
import time

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import redis.asyncio as aioredis

from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AsyncAvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import BaseModel, Field

import streamify.logger  # noqa: F401

from streamify.defs.resources import (
    KafkaConsumerResource,
    RedisResource,
    SchemaRegistryResource,
    StreamingJobConfig,
    get_streaming_config,
)

logger = logging.getLogger(__name__)

BATCH_SIZE_LIMIT = 1000  # Size-based trigger
BATCH_TIME_LIMIT = 1.0  # Timer-based trigger (1 second)


class UserProfile(BaseModel):
    """Typed model for the `user_profiles` Avro topic.

    `alias` is the UPPERCASE field name as registered in the Schema Registry;
    pydantic binds incoming Avro dicts to these typed fields.
    """

    user_id: int | None = Field(default=None, alias="USERID")
    first_name: str | None = Field(default=None, alias="FIRSTNAME")
    last_name: str | None = Field(default=None, alias="LASTNAME")
    gender: str | None = Field(default=None, alias="GENDER")
    city: str | None = Field(default=None, alias="CITY")
    state: str | None = Field(default=None, alias="STATE")
    zip_code: str | None = Field(default=None, alias="ZIP")
    event_time: datetime.datetime | None = Field(default=None, alias="EVENT_TIME")
    ingestion_time: datetime.datetime | None = Field(default=None, alias="INGESTION_TIME")


@dataclass
class SeedRedisResources:
    """Dependency container holding initialized clients for seed_redis."""

    redis_client: aioredis.Redis
    consumer: Consumer
    deserializer: AsyncAvroDeserializer


@asynccontextmanager
async def lifespan_seed_redis(
    config: StreamingJobConfig | None = None,
) -> AsyncIterator[SeedRedisResources]:
    """Assemble and manage lifecycles for Redis, Schema Registry, and Kafka clients."""
    cfg = config or get_streaming_config()

    logger.info(
        "Initializing Redis resource -> Host: %s:%d",
        cfg.redis_host,
        cfg.redis_port,
    )
    redis_res = RedisResource(host=cfg.redis_host, port=cfg.redis_port)

    logger.info("Connecting to Schema Registry at %s...", cfg.schema_registry_url)
    schema_res = SchemaRegistryResource(url=cfg.schema_registry_url)

    kafka_res = KafkaConsumerResource(
        bootstrap_servers=cfg.kafka_bootstrap_servers,
        group_id="async-redis-updater-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    async with redis_res.get_async_client() as redis_client:
        logger.info("✓ Async connected to Redis successfully.")
        with kafka_res.get_consumer(["user_profiles"]) as consumer:
            logger.info("✓ Subscribed Kafka Consumer to topic 'user_profiles'.")
            deserializer = await schema_res.get_avro_deserializer(
                from_dict_fn=lambda data, ctx: UserProfile.model_validate(data)
            )
            logger.info("✓ Deserializer ready. Awaiting messages...")
            try:
                yield SeedRedisResources(
                    redis_client=redis_client,
                    consumer=consumer,
                    deserializer=deserializer,
                )
            finally:
                logger.info("Closing Kafka consumer...")
        logger.info("Closing Redis connection...")
    logger.info("✓ Shutdown complete.")


async def flush_batch_to_redis(
    batch: list[UserProfile],
    redis_client: aioredis.Redis,
    consumer: Consumer,
) -> float:
    """Flush batch to Redis via pipeline and commit offsets."""
    start_flush = time.perf_counter()
    # flush to redis via pipeline
    async with redis_client.pipeline(transaction=False) as pipe:
        for profile in batch:
            if profile.user_id:
                redis_key = f"user:{profile.user_id}"
                pipe.hset(
                    redis_key,
                    mapping=profile.model_dump(
                        mode="json", exclude={"user_id"}, exclude_none=True
                    ),
                )

        # Send all commands to Redis in a single network round-trip
        await pipe.execute()

    duration = time.perf_counter() - start_flush
    # commit offset that we have successfully written to redis
    consumer.commit(asynchronous=True)
    return duration


def should_flush(batch: list, time_since_last_flush_s: float) -> bool:
    return len(batch) >= BATCH_SIZE_LIMIT or (
        len(batch) > 0 and time_since_last_flush_s >= BATCH_TIME_LIMIT
    )


async def run_seed_worker(resources: SeedRedisResources) -> None:
    """Pure pipeline logic - consuming from Kafka and syncing to Redis."""
    batch: list[UserProfile] = []
    last_flush_time = time.perf_counter()

    while True:
        # poll kafka in micro-batches
        # It will return immediately if it collects 100 messages.
        # If less than 100 messages are available, it will wait up to 0.1 seconds
        # to see if more arrive. If the 0.1 second timeout is reached, it returns
        # whatever messages it gathered in that time (even if it's just 1 message,
        # or an empty list []).
        messages = resources.consumer.consume(num_messages=100, timeout=0.1)

        for msg in messages:
            if msg.error():
                logger.error(
                    "Kafka consumer error on message (offset=%s): %s",
                    msg.offset() if hasattr(msg, "offset") else "unknown",
                    msg.error(),
                )
                continue

            topic = msg.topic()
            if topic is None:
                logger.warning("Received message without valid topic name.")
                continue

            try:
                context = SerializationContext(topic, MessageField.VALUE)
                user_data = await resources.deserializer(msg.value(), context)
                if user_data:
                    batch.append(user_data)
            except Exception as exc:
                logger.error(
                    "Failed to deserialize Avro profile at offset %s (topic=%s): %s",
                    msg.offset() if hasattr(msg, "offset") else "unknown",
                    topic,
                    exc,
                )

        current_time = time.perf_counter()
        time_since_last_flush = current_time - last_flush_time

        if should_flush(batch, time_since_last_flush):
            flush_dur = await flush_batch_to_redis(
                batch, resources.redis_client, resources.consumer
            )
            logger.info(
                "✓ Flushed batch of %d records to Redis in one RTT (%.3fs).",
                len(batch),
                flush_dur,
            )
            batch.clear()
            last_flush_time = current_time

        # Yield control back to asyncio loop
        await asyncio.sleep(0.01)


async def main() -> None:
    try:
        async with lifespan_seed_redis() as resources:
            await run_seed_worker(resources)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Stopping async consumer...")


if __name__ == "__main__":
    asyncio.run(main())
