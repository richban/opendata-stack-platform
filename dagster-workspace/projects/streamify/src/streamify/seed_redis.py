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


def transform_avro_to_redis(avro_dict: dict) -> dict:
    """Directly normalizes Avro datatypes and keys to Redis hash format in a single pass."""
    def clean_val(val) -> str:
        if val is None:
            return ""
        if isinstance(val, datetime.datetime):
            return val.isoformat()
        return str(val)

    return {
        "first_name": clean_val(avro_dict.get("FIRSTNAME")),
        "last_name": clean_val(avro_dict.get("LASTNAME")),
        "gender": clean_val(avro_dict.get("GENDER")),
        "city": clean_val(avro_dict.get("CITY")),
        "state": clean_val(avro_dict.get("STATE")),
        "zip_code": clean_val(avro_dict.get("ZIP")),
        "event_time": clean_val(avro_dict.get("EVENT_TIME")),
        "ingestion_time": clean_val(avro_dict.get("INGESTION_TIME")),
    }


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
    deserializer = await AsyncAvroDeserializer(schema_client)  # type: ignore

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
                    for user_data in batch:
                        user_id = user_data.get("USERID")
                        if user_id:
                            redis_key = f"user:{user_id}"
                            redis_hash = transform_avro_to_redis(user_data)
                            pipe.hset(redis_key, mapping=redis_hash)

                    # Send all commands to Redis in a single network round-trip
                    await pipe.execute()

                # commit offset that we have successfully written to redis
                consumer.commit(asynchronous=True)

                print(f"✓ Flushed batch of {len(batch)} records to Redis in one RTT.")
                batch.clear()
                last_flush_time = current_time

            # Yield control back to asyncio loop
            await asyncio.sleep(0.01)

    except KeyboardInterrupt:
        print("\nStopping async consumer...")
    finally:
        consumer.close()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
