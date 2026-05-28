from __future__ import annotations

import json
from typing import Any

import confluent_kafka


class OrderKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str, dry_run: bool = False):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.dry_run = dry_run
        self._producer: confluent_kafka.Producer | None = None

        if not dry_run:
            self._producer = confluent_kafka.Producer({
                "bootstrap.servers": bootstrap_servers,
                "client.id": "order-event-simulator",
                "acks": "all",
            })

    def publish(self, key: str, value: dict[str, Any]) -> None:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"))

        if self.dry_run:
            print(
                json.dumps(
                    {
                        "topic": self.topic,
                        "key": key,
                        "value": value,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return

        assert self._producer is not None

        self._producer.produce(
            topic=self.topic,
            key=key.encode("utf-8"),
            value=payload.encode("utf-8"),
            on_delivery=self._delivery_callback,
        )
        # Poll to handle delivery reports and keep the producer responsive
        self._producer.poll(0)

    def flush(self) -> None:
        if self.dry_run:
            return

        assert self._producer is not None
        self._producer.flush()

    @staticmethod
    def _delivery_callback(err: confluent_kafka.KafkaError | None, msg: confluent_kafka.Message) -> None:
        if err is not None:
            print(f"[simulator] Delivery failed: {err}", flush=True)