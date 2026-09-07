"""Publishes the landing event produced by the Airflow ingestion tasks."""
import json
from datetime import datetime, timezone

KAFKA_BOOTSTRAP_SERVERS = "kafka-broker:9092"
KAFKA_TOPIC = "gates.ingestion.project_monitoring"


def publish_landing_event(dataset: str, run_id: str, manifest_paths: list[str]) -> dict:
    event = {
        "dataset": dataset,
        "run_id": run_id,
        "manifest_paths": manifest_paths,
        "landed_at": datetime.now(timezone.utc).isoformat(),
    }
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    producer.produce(KAFKA_TOPIC, key=dataset, value=json.dumps(event))
    producer.flush()
    print(f"[kafka_publish] Landing event delivered to topic {KAFKA_TOPIC}")
    return event


if __name__ == "__main__":
    publish_landing_event("project_monitoring", "manual-test-run", ["staging/PCHRD/project_monitoring/2026-08-25/"])
