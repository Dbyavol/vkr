from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Any

import pandas as pd


NUMBER_RE = re.compile(r"[-+]?\d[\d\s,]*(?:\.\d+)?|[-+]?\d+(?:,\d+)?")
UNIT_CLEAN_RE = re.compile(r"[^a-zA-Zа-яА-Я₹$€£/%]+")

UNIT_DEFINITIONS: dict[str, tuple[str, str, float | None]] = {
    "m": ("distance", "m", 1.0),
    "meter": ("distance", "m", 1.0),
    "meters": ("distance", "m", 1.0),
    "metre": ("distance", "m", 1.0),
    "metres": ("distance", "m", 1.0),
    "km": ("distance", "km", 1000.0),
    "kms": ("distance", "km", 1000.0),
    "kilometer": ("distance", "km", 1000.0),
    "kilometers": ("distance", "km", 1000.0),
    "kilometre": ("distance", "km", 1000.0),
    "kilometres": ("distance", "km", 1000.0),
    "mi": ("distance", "mi", 1609.344),
    "mile": ("distance", "mi", 1609.344),
    "miles": ("distance", "mi", 1609.344),
    "g": ("weight", "g", 1.0),
    "gram": ("weight", "g", 1.0),
    "grams": ("weight", "g", 1.0),
    "kg": ("weight", "kg", 1000.0),
    "kgs": ("weight", "kg", 1000.0),
    "kilogram": ("weight", "kg", 1000.0),
    "kilograms": ("weight", "kg", 1000.0),
    "lb": ("weight", "lb", 453.59237),
    "lbs": ("weight", "lb", 453.59237),
    "pound": ("weight", "lb", 453.59237),
    "pounds": ("weight", "lb", 453.59237),
    "₹": ("currency", "inr", 1.0),
    "rs": ("currency", "inr", 1.0),
    "inr": ("currency", "inr", 1.0),
    "$": ("currency", "usd", 1.0),
    "usd": ("currency", "usd", 1.0),
    "€": ("currency", "eur", 1.0),
    "eur": ("currency", "eur", 1.0),
    "£": ("currency", "gbp", 1.0),
    "gbp": ("currency", "gbp", 1.0),
}

BASE_UNIT_BY_FAMILY: dict[str, str] = {
    "distance": "m",
    "weight": "g",
}


@dataclass
class ParsedMeasurement:
    value: float | None
    unit: str | None = None
    family: str | None = None


@dataclass
class MeasurementSeriesResult:
    series: pd.Series
    detected_unit_family: str | None = None
    detected_units: list[str] | None = None
    target_unit: str | None = None
    note: str | None = None


def _parse_numeric_token(token: str) -> float | None:
    text = token.strip().replace(" ", "")
    if not text:
        return None
    if "," in text and "." in text:
        normalized = text.replace(",", "")
    elif text.count(",") > 1:
        normalized = text.replace(",", "")
    elif text.count(",") == 1:
        left, right = text.split(",", 1)
        normalized = text.replace(",", "") if len(right) == 3 else f"{left}.{right}"
    else:
        normalized = text
    try:
        return float(normalized)
    except ValueError:
        return None


def _normalize_unit_text(text: str) -> str | None:
    cleaned = UNIT_CLEAN_RE.sub(" ", text.lower()).strip()
    if not cleaned:
        return None
    return cleaned.replace(" ", "")


def parse_measurement(value: Any) -> ParsedMeasurement:
    if value is None:
        return ParsedMeasurement(value=None)
    if isinstance(value, bool):
        return ParsedMeasurement(value=1.0 if value else 0.0)
    if isinstance(value, (int, float)) and not pd.isna(value):
        return ParsedMeasurement(value=float(value))

    text = str(value).strip()
    if not text:
        return ParsedMeasurement(value=None)

    match = NUMBER_RE.search(text)
    if not match:
        return ParsedMeasurement(value=None)

    number = _parse_numeric_token(match.group(0))
    if number is None:
        return ParsedMeasurement(value=None)

    prefix = text[: match.start()]
    suffix = text[match.end() :]
    for candidate in (suffix, prefix):
        normalized = _normalize_unit_text(candidate)
        if not normalized:
            continue
        definition = UNIT_DEFINITIONS.get(normalized)
        if definition:
            family, canonical_unit, _ = definition
            return ParsedMeasurement(value=number, unit=canonical_unit, family=family)

    return ParsedMeasurement(value=number)


def _infer_target_unit(field_key: str, family: str, units: list[str]) -> str | None:
    lowered_key = field_key.lower()
    if family == "distance":
        if "km" in lowered_key:
            return "km"
        if "mile" in lowered_key or lowered_key.endswith("_mi") or lowered_key.startswith("mi_"):
            return "mi"
    if family == "weight":
        if "kg" in lowered_key:
            return "kg"
        if "gram" in lowered_key or lowered_key.endswith("_g") or lowered_key.startswith("g_"):
            return "g"
        if "lb" in lowered_key or "pound" in lowered_key:
            return "lb"
    if units:
        return Counter(units).most_common(1)[0][0]
    return BASE_UNIT_BY_FAMILY.get(family)


def _convert_value(value: float, from_unit: str | None, target_unit: str, family: str) -> float:
    if from_unit is None or from_unit == target_unit:
        return value
    from_definition = UNIT_DEFINITIONS.get(from_unit)
    target_definition = UNIT_DEFINITIONS.get(target_unit)
    if not from_definition or not target_definition:
        return value
    _, _, from_factor = from_definition
    _, _, target_factor = target_definition
    if from_factor is None or target_factor is None:
        return value
    base_value = value * from_factor
    return base_value / target_factor


def coerce_measurement_series(
    series: pd.Series,
    *,
    field_key: str,
    preferred_target_unit: str | None = None,
) -> MeasurementSeriesResult:
    parsed = series.map(parse_measurement)
    numeric_series = parsed.map(lambda item: item.value if item.value is not None else pd.NA)
    units = [item.unit for item in parsed.tolist() if item.unit]
    families = [item.family for item in parsed.tolist() if item.family]

    if not units or not families:
        return MeasurementSeriesResult(series=pd.to_numeric(numeric_series, errors="coerce"))

    family_counts = Counter(families)
    dominant_family = family_counts.most_common(1)[0][0]
    family_units = [item.unit for item in parsed.tolist() if item.family == dominant_family and item.unit]
    target_unit = preferred_target_unit or _infer_target_unit(field_key, dominant_family, family_units)
    detected_units = sorted(set(units))

    if target_unit and dominant_family in BASE_UNIT_BY_FAMILY:
        converted = parsed.map(
            lambda item: _convert_value(item.value, item.unit, target_unit, dominant_family)
            if item.value is not None
            else pd.NA
        )
        return MeasurementSeriesResult(
            series=pd.to_numeric(converted, errors="coerce"),
            detected_unit_family=dominant_family,
            detected_units=detected_units,
            target_unit=target_unit,
            note=f"Embedded units detected ({', '.join(detected_units)}) and converted to {target_unit}.",
        )

    return MeasurementSeriesResult(
        series=pd.to_numeric(numeric_series, errors="coerce"),
        detected_unit_family=dominant_family,
        detected_units=detected_units,
        target_unit=target_unit,
        note=f"Embedded units detected ({', '.join(detected_units)}); numeric values extracted.",
    )
