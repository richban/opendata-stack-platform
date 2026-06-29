import asyncio
import os
import time
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import AsyncSchemaRegistryClient
from confluent_kafka.schema_registry.avro import AsyncAvroDeserializer
import redis.asyncio as aioredis
from confluent_kafka.serialization import MessageField, SerializationContext
from streamify.defs.resources import (
    create_s3_resource,
    create_spark_session,
    create_streaming_config,
)
from dataclasses import dataclass, asdict
import datetime

BATCH_SIZE_LIMIT = 1000  # Size-based trigger
BATCH_TIME_LIMIT = 1.0  # Timer-based trigger (1 second)


@dataclass
class UserProfile:
    user_id: int
    first_name: str
    last_name: str
    gender: str
    city: str
    state: str
    zip_code: str
    event_time: str
    ingestion_time: str

    @classmethod
    def from_avro(cls, avro_dict: dict) -> "UserProfile":
        """Parses raw Avro dictionary keys (case-insensitive/capitalized) and normalizes types."""

        def clean_val(val) -> str:
            if val is None:
                return ""
            if isinstance(val, datetime.datetime):
                return val.isoformat()  # Convert Datetime to string
            return str(val)

        return cls(
            user_id=int(avro_dict.get("USERID") or 0),
            first_name=clean_val(avro_dict.get("FIRSTNAME")),
            last_name=clean_val(avro_dict.get("LASTNAME")),
            gender=clean_val(avro_dict.get("GENDER")),
            city=clean_val(avro_dict.get("CITY")),
            state=clean_val(avro_dict.get("STATE")),
            zip_code=clean_val(avro_dict.get("ZIP")),
            event_time=clean_val(avro_dict.get("EVENT_TIME")),
            ingestion_time=clean_val(avro_dict.get("INGESTION_TIME")),
        )

    def to_redis_hash(self) -> dict:
        """Returns a flat dictionary of strings suitable for Redis HSET (excludes ID)."""
        data = asdict(self)
        del data["user_id"]  # ID is used in the Redis key, not the hash itself
        return data


def avro_to_user_profile(obj, ctx):
    if obj is None:
        return None
    return UserProfile.from_avro(obj)


async def main():
    cfg = create_streaming_config()

    redis_client = aioredis.Redis(
        host=cfg.redis_host,
        port=cfg.redis_port,
        decode_responses=True,
    )
    await redis_client.ping()
    print("✓ Async connected to Redis.")

    schema_client = AsyncSchemaRegistryClient({"url": cfg.schema_registry_url})
    deserializer = await AsyncAvroDeserializer(
        schema_client, from_dict=avro_to_user_profile
    )  # type: ignore

    consumer_config = {
        "bootstrap.servers": cfg.kafka_bootstrap_servers,
        "group.id": "async-redis-updater-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # We commit offsets manually AFTER flushing to Redis
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(["user_profiles"])

    print("✓ Async Consumer subscribed to 'user_profiles' topic.")

    batch = []
    last_flush_time = time.perf_counter()

    try:
        while True:
            # poll kafka in micro-batches
            # It will return immediately if it collects 100 messages.
            # If less than 100 messages are available, it will wait up to 0.1 seconds to see if more arrive.
            # If the 0.1 second timeout is reached, it returns whatever messages it gathered in that time (even if it's just 1 message, or an empty list []).
            messages = consumer.consume(num_messages=100, timeout=0.1)

            for msg in messages:
                if msg.error():
                    # log error
                    print(f"Error: {msg.error()}")  # Should write to DLQ?
                    continue

                topic = msg.topic()
                if topic is None:
                    print(
                        f"Error: {msg} has no valid topic {topic} name"
                    )  # Should write to DLQ?
                    continue  # Skip if the message has no valid topic name

                context = SerializationContext(topic, MessageField.VALUE)
                user_data = await deserializer(msg.value(), context)

                if user_data:
                    batch.append(user_data)

            current_time = time.perf_counter()
            time_since_last_flush = current_time - last_flush_time

            if len(batch) >= BATCH_SIZE_LIMIT or (
                len(batch) > 0 and time_since_last_flush >= BATCH_TIME_LIMIT
            ):
                # flush to redis via pipeline
                async with redis_client.pipeline(transaction=False) as pipe:
                    for user in batch:
                        if user:
                            redis_key = f"user:{user.user_id}"
                            pipe.hset(redis_key, mapping=user.to_redis_hash())

                    # Send all commands to Redis in a single network round-trip
                    await pipe.execute()

                # commit offset that we have successfully written to redis
                consumer.commit(asynchronous=True)

                print(f"✓ Flushed batch of {len(batch)} records to Redis in one RTT.")
                batch.clear()
                last_flush_time = current_time

    except KeyboardInterrupt:
        print("\nStopping async consumer...")
    finally:
        consumer.close()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
