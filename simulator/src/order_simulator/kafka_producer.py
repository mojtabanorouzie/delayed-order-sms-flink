from __future__ import annotations

import json
from typing import Any


class OrderKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str, dry_run: bool = False):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.dry_run = dry_run
        self._producer = None

        if not dry_run:
            from confluent_kafka import Producer

            self._producer = Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "client.id": "order-event-simulator",
                    "acks": "all",
                    "enable.idempotence": True,
                }
            )

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
        )

        self._producer.poll(0)

    def flush(self) -> None:
        if self.dry_run:
            return

        assert self._producer is not None
        self._producer.flush()