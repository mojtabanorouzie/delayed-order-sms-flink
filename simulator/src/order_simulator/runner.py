from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
import copy
import random
import re
import time
from typing import Any

from order_simulator.config import SimulatorConfig
from order_simulator.kafka_producer import OrderKafkaProducer
from order_simulator.scenario_loader import ScenarioLoader
from order_simulator.template_renderer import render_template
from order_simulator.time_utils import resolve_dynamic_times, utc_now, parse_offset, to_iso_z


@dataclass(frozen=True)
class OrderContext:
    order_index: int
    order_id: str
    customer_id: str
    store_id: str
    base_time: datetime

    def as_template_context(self) -> dict[str, str]:
        return {
            "orderId": self.order_id,
            "customerId": self.customer_id,
            "storeId": self.store_id,
        }


class SimulatorRunner:
    def __init__(
        self,
        config: SimulatorConfig,
        scenario_loader: ScenarioLoader,
        producer: OrderKafkaProducer,
    ):
        self.config = config
        self.scenario_loader = scenario_loader
        self.producer = producer
        self.random = random.Random(config.random_seed)

    def run(self) -> None:
        scenario = self.scenario_loader.load(self.config.scenario)

        scenario_name = scenario.get("scenarioName", self.config.scenario)
        print(f"[simulator] starting scenario={scenario_name}")
        print(f"[simulator] orders_count={self.config.orders_count}")
        print(f"[simulator] topic={self.config.orders_topic}")
        print(f"[simulator] dry_run={self.config.dry_run}")

        if self._is_mixed_scenario(scenario):
            self._run_mixed_scenario(scenario)
        else:
            self._run_single_scenario(scenario)

        self.producer.flush()
        print("[simulator] completed")

    def _run_single_scenario(self, scenario: dict[str, Any]) -> None:
        contexts = self._create_order_contexts(
            scenario_name=scenario.get("scenarioName", self.config.scenario),
            orders_count=self.config.orders_count,
        )

        self._run_orders_with_scenario(contexts, scenario)

    def _run_mixed_scenario(self, mixed_scenario: dict[str, Any]) -> None:
        included_scenarios = mixed_scenario.get("includedScenarios", [])

        if not included_scenarios:
            raise ValueError("Mixed scenario must define includedScenarios")

        loaded_scenarios = self.scenario_loader.load_many(included_scenarios)
        assignments = self._assign_scenarios(mixed_scenario, included_scenarios)

        tasks: list[tuple[OrderContext, dict[str, Any]]] = []

        for index, scenario_name in enumerate(assignments, start=1):
            scenario = loaded_scenarios[scenario_name]
            context = self._create_order_context(
                order_index=index,
                scenario_name=scenario_name,
            )
            tasks.append((context, scenario))

        self._execute_tasks(tasks)

    def _run_orders_with_scenario(
        self,
        contexts: list[OrderContext],
        scenario: dict[str, Any],
    ) -> None:
        tasks = [(context, scenario) for context in contexts]
        self._execute_tasks(tasks)

    def _execute_tasks(self, tasks: list[tuple[OrderContext, dict[str, Any]]]) -> None:
        max_workers = max(1, self.config.max_workers)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._run_single_order, context, scenario)
                for context, scenario in tasks
            ]

            for future in as_completed(futures):
                future.result()

    def _run_single_order(self, context: OrderContext, scenario: dict[str, Any]) -> None:
        events = scenario.get("events", [])

        if not events:
            raise ValueError(f"Scenario has no events: {scenario.get('scenarioName')}")

        published_by_step: dict[int, dict[str, Any]] = {}

        for event_definition in events:
            step = int(event_definition.get("step", 0))
            delay_ms = int(event_definition.get("delayAfterPreviousEventMs", 0))
            adjusted_delay_ms = int(delay_ms * self.config.event_delay_multiplier)

            if adjusted_delay_ms > 0:
                time.sleep(adjusted_delay_ms / 1000.0)

            if self.config.pause_after_step and step == self.config.pause_after_step:
                self._pause_for_failure_testing(step)

            if "duplicateOfStep" in event_definition:
                duplicate_step = int(event_definition["duplicateOfStep"])

                if duplicate_step not in published_by_step:
                    raise ValueError(
                        f"duplicateOfStep={duplicate_step} was not published yet "
                        f"for orderId={context.order_id}"
                    )

                value = copy.deepcopy(published_by_step[duplicate_step])
            else:
                value_template = event_definition.get("value")

                if not value_template:
                    raise ValueError(
                        f"Event step={step} must contain value or duplicateOfStep"
                    )

                value = self._build_event_value(value_template, context, event_definition)
                published_by_step[step] = copy.deepcopy(value)

            message_key = event_definition.get("messageKey", "{{orderId}}")
            key = render_template(message_key, context.as_template_context())

            self.producer.publish(key=key, value=value)

    def _build_event_value(
        self,
        value_template: dict[str, Any],
        context: OrderContext,
        event_definition: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        rendered = render_template(value_template, context.as_template_context())
        resolved = resolve_dynamic_times(rendered, context.base_time)
        
        # Apply etaOffset if present in event definition
        if event_definition and "etaOffset" in event_definition and "expectedDeliveryTime" in resolved:
            try:
                eta_offset = parse_offset(event_definition["etaOffset"])
                adjusted_time = context.base_time + eta_offset
                resolved["expectedDeliveryTime"] = to_iso_z(adjusted_time)
            except (ValueError, TypeError) as e:
                print(f"Warning: Failed to apply etaOffset: {e}")

        return resolved

    def _create_order_contexts(
        self,
        scenario_name: str,
        orders_count: int,
    ) -> list[OrderContext]:
        return [
            self._create_order_context(index, scenario_name)
            for index in range(1, orders_count + 1)
        ]

    def _create_order_context(
        self,
        order_index: int,
        scenario_name: str,
    ) -> OrderContext:
        normalized_scenario_name = self._normalize_name(scenario_name)

        return OrderContext(
            order_index=order_index,
            order_id=f"{self.config.order_id_prefix}-{normalized_scenario_name}-{order_index:04d}",
            customer_id=f"cus-{order_index:04d}",
            store_id=f"store-{((order_index - 1) % 10) + 1:03d}",
            base_time=utc_now(),
        )

    def _assign_scenarios(
        self,
        mixed_scenario: dict[str, Any],
        included_scenarios: list[str],
    ) -> list[str]:
        distribution = mixed_scenario.get("distribution", {})

        if not distribution:
            return [
                self.random.choice(included_scenarios)
                for _ in range(self.config.orders_count)
            ]

        weighted_scenarios: list[tuple[str, int]] = []

        distribution_mapping = {
            "onTimePercentage": "on-time-orders",
            "delayedPercentage": "delayed-orders",
            "cancelledPercentage": "cancelled-orders",
            "etaUpdatedPercentage": "eta-updated-orders",
            "duplicatePercentage": "duplicate-events",
            "outOfOrderPercentage": "out-of-order-updates",
        }

        for distribution_key, scenario_name in distribution_mapping.items():
            weight = int(distribution.get(distribution_key, 0))

            if weight > 0 and scenario_name in included_scenarios:
                weighted_scenarios.append((scenario_name, weight))

        if not weighted_scenarios:
            return [
                self.random.choice(included_scenarios)
                for _ in range(self.config.orders_count)
            ]

        total_weight = sum(weight for _, weight in weighted_scenarios)
        if total_weight <= 0:
            return [
                self.random.choice(included_scenarios)
                for _ in range(self.config.orders_count)
            ]

        fractional_counts: list[tuple[str, int, float]] = []
        assigned_counts: dict[str, int] = {}

        for scenario_name, weight in weighted_scenarios:
            exact = self.config.orders_count * weight / total_weight
            floor_count = int(exact)
            fractional_counts.append((scenario_name, floor_count, exact - floor_count))
            assigned_counts[scenario_name] = floor_count

        remaining = self.config.orders_count - sum(count for _, count, _ in fractional_counts)
        fractional_counts.sort(key=lambda item: item[2], reverse=True)

        for scenario_name, _, _ in fractional_counts[:remaining]:
            assigned_counts[scenario_name] += 1

        assignments: list[str] = []
        for scenario_name, _, _ in fractional_counts:
            assignments.extend([scenario_name] * assigned_counts[scenario_name])

        return assignments

    def _pause_for_failure_testing(self, step: int) -> None:
        if self.config.pause_duration_seconds > 0:
            print(
                f"[simulator] pausing after step={step} "
                f"for {self.config.pause_duration_seconds}s"
            )
            time.sleep(self.config.pause_duration_seconds)

    @staticmethod
    def _is_mixed_scenario(scenario: dict[str, Any]) -> bool:
        return bool(scenario.get("includedScenarios"))

    @staticmethod
    def _normalize_name(value: str) -> str:
        value = value.removesuffix(".json")
        value = re.sub(r"[^a-zA-Z0-9]+", "-", value)
        return value.strip("-").lower()