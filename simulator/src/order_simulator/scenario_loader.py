from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class ScenarioLoader:
    def __init__(self, scenarios_dir: Path | None = None):
        if scenarios_dir is None:
            # simulator/src/order_simulator/scenario_loader.py
            # parents[2] => simulator/
            scenarios_dir = Path(__file__).resolve().parents[2] / "scenarios"

        self.scenarios_dir = scenarios_dir

    def load(self, scenario_name: str) -> dict[str, Any]:
        normalized_name = scenario_name

        if normalized_name.endswith(".json"):
            normalized_name = normalized_name.removesuffix(".json")

        path = self.scenarios_dir / f"{normalized_name}.json"

        if not path.exists():
            raise FileNotFoundError(f"Scenario file not found: {path}")

        with path.open("r", encoding="utf-8") as file:
            return json.load(file)

    def load_many(self, scenario_names: list[str]) -> dict[str, dict[str, Any]]:
        return {name: self.load(name) for name in scenario_names}