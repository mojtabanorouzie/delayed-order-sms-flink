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
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                client_id="order-event-simulator",
                acks="all",
                enable_idempotence=True,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
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

        self._producer.send(
            topic=self.topic,
            key=key.encode("utf-8"),
            value=payload.encode("utf-8"),
        )

    def flush(self) -> None:
        if self.dry_run:
            return

        assert self._producer is not None
        self._producer.flush()