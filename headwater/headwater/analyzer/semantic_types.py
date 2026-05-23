"""Generic semantic type detectors for format and distribution signals."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field

from headwater.core.models import ColumnProfile


@dataclass(frozen=True)
class SemanticTypeEvidence:
    """Evidence from a generic semantic type detector."""

    semantic_type: str
    confidence: float
    support_count: int
    sample_size: int
    sensitive: bool = False
    detector: str = ""
    conflicts: list[str] = field(default_factory=list)

    def model_dump(self) -> dict:
        return asdict(self)


_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_URL_RE = re.compile(r"^https?://[^\s/$.?#].[^\s]*$", re.I)
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)
_PHONE_RE = re.compile(r"^\+?[0-9][0-9().\-\s]{6,}[0-9]$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[ tT]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?(Z|[+-]\d{2}:?\d{2})?$"
)
_CURRENCY_CODE_RE = re.compile(r"^[A-Z]{3}$")
_PERCENT_RE = re.compile(r"^-?\d+(\.\d+)?%$")
_IBAN_RE = re.compile(r"^[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}$")
_POSTAL_CODE_RE = re.compile(r"^[A-Z0-9][A-Z0-9 -]{2,11}$", re.I)

_CURRENCY_CODES = {
    "AUD",
    "CAD",
    "CHF",
    "CNY",
    "EUR",
    "GBP",
    "HKD",
    "INR",
    "JPY",
    "NZD",
    "SGD",
    "USD",
    "ZAR",
}

_MONETARY_NAME_RE = re.compile(r"(amount|currency|money|price|cost|revenue|total|subtotal)", re.I)
_LAT_NAME_RE = re.compile(r"(^lat$|latitude)", re.I)
_LNG_NAME_RE = re.compile(r"(^lon$|^lng$|longitude)", re.I)
_PERCENT_NAME_RE = re.compile(r"(percent|percentage|pct|rate|ratio)", re.I)
_COUNTRY_NAME_RE = re.compile(r"country(_code)?$", re.I)
_POSTAL_NAME_RE = re.compile(r"(postal|postcode|zip)(_code)?$", re.I)


def detect_semantic_types(
    column_name: str,
    dtype: str | None,
    profile: ColumnProfile | None,
) -> list[SemanticTypeEvidence]:
    """Return generic semantic type evidence ordered by confidence."""
    detections: list[SemanticTypeEvidence] = []
    values = _profile_values(profile)
    sample_size = sum(count for _value, count in values)

    if profile and profile.detected_pattern:
        detections.append(
            SemanticTypeEvidence(
                semantic_type=_normalize_profile_pattern(profile.detected_pattern),
                confidence=0.9,
                support_count=sample_size,
                sample_size=sample_size,
                sensitive=profile.detected_pattern in {"email", "phone", "iban"},
                detector="profile_pattern",
            )
        )

    if values:
        detections.extend(_value_shape_detections(values))

    detections.extend(_range_and_name_detections(column_name, dtype, profile))
    return _dedupe_and_mark_conflicts(detections)


def primary_semantic_type(
    column_name: str,
    dtype: str | None,
    profile: ColumnProfile | None,
) -> SemanticTypeEvidence | None:
    detections = detect_semantic_types(column_name, dtype, profile)
    return detections[0] if detections else None


def _value_shape_detections(values: list[tuple[str, int]]) -> list[SemanticTypeEvidence]:
    checks = [
        ("email", _EMAIL_RE.match, True),
        ("url", _URL_RE.match, False),
        ("uuid", _UUID_RE.match, True),
        ("phone", _PHONE_RE.match, True),
        ("iso_datetime", _ISO_DATETIME_RE.match, False),
        ("iso_date", _ISO_DATE_RE.match, False),
        ("percentage", _PERCENT_RE.match, False),
        ("iban", _valid_iban, True),
        ("currency_code", _is_currency_code, False),
        ("postal_code", _POSTAL_CODE_RE.match, False),
    ]
    sample_size = sum(count for _value, count in values)
    detections: list[SemanticTypeEvidence] = []
    for semantic_type, predicate, sensitive in checks:
        support = sum(count for value, count in values if predicate(value.strip()))
        if support <= 0:
            continue
        confidence = support / max(sample_size, 1)
        if confidence < 0.6:
            continue
        detections.append(
            SemanticTypeEvidence(
                semantic_type=semantic_type,
                confidence=round(min(0.99, 0.55 + confidence * 0.44), 3),
                support_count=support,
                sample_size=sample_size,
                sensitive=sensitive,
                detector="value_shape",
            )
        )
    return detections


def _range_and_name_detections(
    column_name: str,
    dtype: str | None,
    profile: ColumnProfile | None,
) -> list[SemanticTypeEvidence]:
    detections: list[SemanticTypeEvidence] = []
    lower_dtype = (dtype or "").lower()
    numeric = any(
        token in lower_dtype
        for token in ("int", "float", "double", "decimal", "numeric")
    )
    sample_size = _sample_size(profile)
    if _LAT_NAME_RE.search(column_name) and numeric and _in_range(profile, -90, 90):
        detections.append(_range_evidence("latitude", sample_size, 0.94))
    if _LNG_NAME_RE.search(column_name) and numeric and _in_range(profile, -180, 180):
        detections.append(_range_evidence("longitude", sample_size, 0.94))
    if _PERCENT_NAME_RE.search(column_name) and numeric and _in_range(profile, 0, 100):
        detections.append(_range_evidence("percentage", sample_size, 0.82))
    if _MONETARY_NAME_RE.search(column_name) and numeric:
        detections.append(_range_evidence("monetary_amount", sample_size, 0.68))
    if _COUNTRY_NAME_RE.search(column_name):
        detections.append(_range_evidence("country_code", sample_size, 0.64))
    if _POSTAL_NAME_RE.search(column_name):
        detections.append(_range_evidence("postal_code", sample_size, 0.64))
    return detections


def _range_evidence(
    semantic_type: str,
    sample_size: int,
    confidence: float,
) -> SemanticTypeEvidence:
    return SemanticTypeEvidence(
        semantic_type=semantic_type,
        confidence=confidence,
        support_count=sample_size,
        sample_size=sample_size,
        sensitive=semantic_type in {"iban", "email", "phone"},
        detector="name_range",
    )


def _profile_values(profile: ColumnProfile | None) -> list[tuple[str, int]]:
    values = []
    for value, count in (profile.top_values if profile and profile.top_values else []):
        text = str(value).strip()
        if text:
            values.append((text, int(count)))
    return values


def _sample_size(profile: ColumnProfile | None) -> int:
    if profile is None:
        return 0
    if profile.top_values:
        return sum(int(count) for _value, count in profile.top_values)
    return int(profile.distinct_count or 0)


def _in_range(profile: ColumnProfile | None, lower: float, upper: float) -> bool:
    if profile is None or profile.min_value is None or profile.max_value is None:
        return True
    return lower <= profile.min_value <= upper and lower <= profile.max_value <= upper


def _normalize_profile_pattern(pattern: str) -> str:
    return "iso_date" if pattern == "date" else pattern


def _is_currency_code(value: str) -> bool:
    return bool(_CURRENCY_CODE_RE.match(value)) and value.upper() in _CURRENCY_CODES


def _valid_iban(value: str) -> bool:
    compact = re.sub(r"\s+", "", value).upper()
    if not _IBAN_RE.match(compact):
        return False
    rearranged = compact[4:] + compact[:4]
    numeric = "".join(str(ord(ch) - 55) if ch.isalpha() else ch for ch in rearranged)
    return int(numeric) % 97 == 1


def _dedupe_and_mark_conflicts(
    detections: list[SemanticTypeEvidence],
) -> list[SemanticTypeEvidence]:
    by_type: dict[str, SemanticTypeEvidence] = {}
    for detection in detections:
        current = by_type.get(detection.semantic_type)
        if current is None or detection.confidence > current.confidence:
            by_type[detection.semantic_type] = detection
    ordered = sorted(by_type.values(), key=lambda item: item.confidence, reverse=True)
    if len(ordered) <= 1:
        return ordered
    types = [item.semantic_type for item in ordered]
    return [
        SemanticTypeEvidence(
            semantic_type=item.semantic_type,
            confidence=item.confidence,
            support_count=item.support_count,
            sample_size=item.sample_size,
            sensitive=item.sensitive,
            detector=item.detector,
            conflicts=[other for other in types if other != item.semantic_type],
        )
        for item in ordered
    ]
