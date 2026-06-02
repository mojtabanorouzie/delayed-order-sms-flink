from __future__ import annotations

from datetime import datetime, timedelta, timezone
import re
from typing import Any


_TIME_EXPRESSION_PATTERN = re.compile(r"^now(?P<sign>[+-])?(?P<amount>\d+)?(?P<unit>ms|s|m|h|d)?$")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_dynamic_time(value: str, base_time: datetime) -> str:
    """
    Resolves dynamic expressions:
      now
      now+500ms
      now+10s
      now+2m
      now+1h
      now+1d
    """
    match = _TIME_EXPRESSION_PATTERN.match(value)

    if not match:
        return value

    sign = match.group("sign")
    amount = match.group("amount")
    unit = match.group("unit")

    if not sign:
        return to_iso_z(base_time)

    amount_int = int(amount or "0")

    if unit == "ms":
        delta = timedelta(milliseconds=amount_int)
    elif unit == "s":
        delta = timedelta(seconds=amount_int)
    elif unit == "m":
        delta = timedelta(minutes=amount_int)
    elif unit == "h":
        delta = timedelta(hours=amount_int)
    elif unit == "d":
        delta = timedelta(days=amount_int)
    else:
        delta = timedelta(seconds=amount_int)

    resolved = base_time + delta if sign == "+" else base_time - delta

    return to_iso_z(resolved)


def parse_offset(offset_str: str) -> timedelta:
    """
    Parses offset expressions like "-30s", "+60s", "now-5m", etc.
    Returns a timedelta that represents this offset from now.
    
    Supports:
      -500ms
      -10s
      -2m
      -1h
      -1d
      +60s (positive offsets)
    """
    if not offset_str:
        return timedelta()
    
    # Handle "now-5s" style (extract the offset part)
    if offset_str.startswith("now"):
        offset_str = offset_str[3:]  # Remove "now" prefix
    
    match = _TIME_EXPRESSION_PATTERN.match(f"now{offset_str}")
    
    if not match:
        raise ValueError(f"Invalid offset expression: {offset_str}")
    
    sign = match.group("sign")
    amount = match.group("amount")
    unit = match.group("unit")
    
    if not sign:
        return timedelta()
    
    amount_int = int(amount or "0")
    
    if unit == "ms":
        delta = timedelta(milliseconds=amount_int)
    elif unit == "s":
        delta = timedelta(seconds=amount_int)
    elif unit == "m":
        delta = timedelta(minutes=amount_int)
    elif unit == "h":
        delta = timedelta(hours=amount_int)
    elif unit == "d":
        delta = timedelta(days=amount_int)
    else:
        delta = timedelta(seconds=amount_int)
    
    return delta if sign == "+" else -delta


def resolve_dynamic_times(payload: Any, base_time: datetime) -> Any:
    if isinstance(payload, dict):
        return {key: resolve_dynamic_times(value, base_time) for key, value in payload.items()}

    if isinstance(payload, list):
        return [resolve_dynamic_times(item, base_time) for item in payload]

    if isinstance(payload, str):
        return parse_dynamic_time(payload, base_time)

    return payload