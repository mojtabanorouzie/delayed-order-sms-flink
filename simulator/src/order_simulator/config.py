from dataclasses import dataclass
import os


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    return value.lower() in ("1", "true", "yes", "y")


@dataclass(frozen=True)
class SimulatorConfig:
    kafka_bootstrap_servers: str
    orders_topic: str
    scenario: str
    orders_count: int
    order_id_prefix: str
    event_delay_multiplier: float
    dry_run: bool
    random_seed: int
    max_workers: int
    pause_after_step: int
    pause_duration_seconds: int

    @staticmethod
    def from_env() -> "SimulatorConfig":
        return SimulatorConfig(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            orders_topic=os.getenv("ORDERS_TOPIC", "Orders"),
            scenario=os.getenv("SCENARIO", "mixed-orders"),
            orders_count=int(os.getenv("ORDERS_COUNT", "100")),
            order_id_prefix=os.getenv("ORDER_ID_PREFIX", "ord"),
            event_delay_multiplier=float(os.getenv("EVENT_DELAY_MULTIPLIER", "1.0")),
            dry_run=_env_bool("DRY_RUN", False),
            random_seed=int(os.getenv("RANDOM_SEED", "42")),
            max_workers=int(os.getenv("MAX_WORKERS", "10")),
            pause_after_step=int(os.getenv("PAUSE_AFTER_STEP", "0")),
            pause_duration_seconds=int(os.getenv("PAUSE_DURATION_SECONDS", "0")),
        )

    def override(
        self,
        scenario: str | None = None,
        orders_count: int | None = None,
        dry_run: bool | None = None,
        kafka_bootstrap_servers: str | None = None,
        orders_topic: str | None = None,
    ) -> "SimulatorConfig":
        return SimulatorConfig(
            kafka_bootstrap_servers=kafka_bootstrap_servers or self.kafka_bootstrap_servers,
            orders_topic=orders_topic or self.orders_topic,
            scenario=scenario or self.scenario,
            orders_count=orders_count if orders_count is not None else self.orders_count,
            order_id_prefix=self.order_id_prefix,
            event_delay_multiplier=self.event_delay_multiplier,
            dry_run=dry_run if dry_run is not None else self.dry_run,
            random_seed=self.random_seed,
            max_workers=self.max_workers,
            pause_after_step=self.pause_after_step,
            pause_duration_seconds=self.pause_duration_seconds,
        )