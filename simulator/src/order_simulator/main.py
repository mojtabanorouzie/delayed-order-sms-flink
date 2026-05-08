from __future__ import annotations

import argparse

from order_simulator.config import SimulatorConfig
from order_simulator.kafka_producer import OrderKafkaProducer
from order_simulator.runner import SimulatorRunner
from order_simulator.scenario_loader import ScenarioLoader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="order-simulator",
        description="Generate order state snapshots and publish them to Kafka.",
    )

    parser.add_argument(
        "--scenario",
        required=False,
        help="Scenario name from simulator/scenarios without .json",
    )

    parser.add_argument(
        "--orders-count",
        type=int,
        required=False,
        help="Number of orders to generate",
    )

    parser.add_argument(
        "--kafka-bootstrap-servers",
        required=False,
        help="Kafka bootstrap servers",
    )

    parser.add_argument(
        "--orders-topic",
        required=False,
        help="Target Kafka topic. Default: Orders",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print messages instead of publishing to Kafka",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = SimulatorConfig.from_env().override(
        scenario=args.scenario,
        orders_count=args.orders_count,
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
        orders_topic=args.orders_topic,
        dry_run=True if args.dry_run else None,
    )

    producer = OrderKafkaProducer(
        bootstrap_servers=config.kafka_bootstrap_servers,
        topic=config.orders_topic,
        dry_run=config.dry_run,
    )

    runner = SimulatorRunner(
        config=config,
        scenario_loader=ScenarioLoader(),
        producer=producer,
    )

    runner.run()


if __name__ == "__main__":
    main()