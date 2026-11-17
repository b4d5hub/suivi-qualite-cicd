import json, os
from pathlib import Path
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()


def publish_messages(messages):
    # PUBLISH JSON MSG AND VERIFY DELIVERY

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "github_ci_colletor",
            "acks": "all",
            "enable.idempotence": True,
            "broker.address.family": "v4",
        }
    )
    delivered = []
    failed = []

    # WHEN KAFKA CONFIRMS DELIVERY OR GIVES ERROR
    def delivery_report(error, message):

        if error is not None:
            failed.append(str(error))
            print(f"Delivery Failed: {error}")
        else:
            delivered.append(message)
            print(
                f"Delivered to : {message.topic()}"
                f"partition : {message.partition()}"
                f"offset : {message.offset()}"
            )

    for topic, key, payload in messages:
        json_value = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        producer.produce(
            topic=topic,
            key=str(key).encode("utf-8"),
            value=json_value,
            on_delivery=delivery_report,
        )
        # PROCESS PENDING DELIVERY CALLBACKS
        producer.poll(0)

    # W8 FOR QUEUED MSG TO FINISH
    remaining = producer.flush(30)

    if remaining or failed or len(delivered) != len(messages):
        raise RuntimeError("Some Kafka messages were not confirmed delivered")
    print(f"\nSuccessfully delivered : {len(delivered)}")
