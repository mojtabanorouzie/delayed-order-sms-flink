from __future__ import annotations

from typing import Any


def render_template_value(value: str, context: dict[str, str]) -> str:
    rendered = value

    for key, replacement in context.items():
        rendered = rendered.replace("{{" + key + "}}", replacement)

    return rendered


def render_template(payload: Any, context: dict[str, str]) -> Any:
    if isinstance(payload, dict):
        return {key: render_template(value, context) for key, value in payload.items()}

    if isinstance(payload, list):
        return [render_template(item, context) for item in payload]

    if isinstance(payload, str):
        return render_template_value(payload, context)

    return payload