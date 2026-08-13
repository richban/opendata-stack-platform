import asyncio
import datetime
import logging
import time

from collections.abc import Coroutine
from typing import Any, cast

import redis.asyncio as aioredis

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry.avro import AsyncAvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import BaseModel, Field

from streamify.defs.resources import get_streaming_config
from streamify.log import configure_logging

# Configure structured logging
configure_logging()
logger = logging.getLogger("streamify.seed_redis")

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


async def flush_batch_to_redis(batch: list[UserProfile], redis_client, consumer) -> float:
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


async def main():
    cfg = get_streaming_config()

    logger.info(
        "Initializing Redis connection -> Host: %s:%d",
        cfg.redis_host,
        cfg.redis_port,
    )
    redis_client = aioredis.Redis(
        host=cfg.redis_host,
        port=cfg.redis_port,
        decode_responses=True,
    )
    await redis_client.ping()
    logger.info("✓ Async connected to Redis successfully.")

    logger.info("Connecting to Schema Registry at %s...", cfg.schema_registry_url)
    schema_client = AsyncSchemaRegistryClient({"url": cfg.schema_registry_url})
    deserializer = await cast(
        Coroutine[Any, Any, AsyncAvroDeserializer],
        AsyncAvroDeserializer(
            schema_client, from_dict=lambda data, ctx: UserProfile.model_validate(data)
        ),
    )

    consumer_config = {
        "bootstrap.servers": cfg.kafka_bootstrap_servers,
        "group.id": "async-redis-updater-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # We commit offsets manually AFTER flushing to Redis
    }
    logger.info(
        "Subscribing Kafka Consumer (group: %s) to topic 'user_profiles'...",
        consumer_config["group.id"],
    )
    consumer = Consumer(consumer_config)
    consumer.subscribe(["user_profiles"])

    logger.info("✓ Consumer subscribed. Awaiting messages...")

    batch = []
    last_flush_time = time.perf_counter()

    try:
        while True:
            # poll kafka in micro-batches
            # It will return immediately if it collects 100 messages.
            # If less than 100 messages are available, it will wait up to 0.1 seconds
            # to see if more arrive. If the 0.1 second timeout is reached, it returns
            # whatever messages it gathered in that time (even if it's just 1 message,
            # or an empty list []).
            messages = consumer.consume(num_messages=100, timeout=0.1)

            for msg in messages:
                if msg.error():
                    # log error
                    # Should write to DLQ?
                    logger.error("Kafka consumer error: %s", msg.error())
                    continue

                topic = msg.topic()
                if topic is None:
                    # Should write to DLQ?
                    logger.warning("Received message without valid topic name.")
                    continue  # Skip if the message has no valid topic name

                context = SerializationContext(topic, MessageField.VALUE)
                user_data = await deserializer(msg.value(), context)

                if user_data:
                    batch.append(user_data)

            current_time = time.perf_counter()
            time_since_last_flush = current_time - last_flush_time

            if should_flush(batch, time_since_last_flush):
                flush_dur = await flush_batch_to_redis(batch, redis_client, consumer)
                logger.info(
                    "✓ Flushed batch of %d records to Redis in one RTT (%.3fs).",
                    len(batch),
                    flush_dur,
                )
                batch.clear()
                last_flush_time = current_time

            # Yield control back to asyncio loop
            await asyncio.sleep(0.01)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received. Stopping async consumer...")
    finally:
        logger.info("Closing Kafka consumer and Redis client connection...")
        consumer.close()
        await redis_client.aclose()
        logger.info("✓ Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
