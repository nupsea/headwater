"""Typed event names shared by services and routes."""

from __future__ import annotations

from enum import StrEnum


class EventType(StrEnum):
    SOURCE_REGISTERED = "source_registered"
    CONNECTION_TESTED = "connection_tested"
    CONNECTION_TEST_FAILED = "connection_test_failed"
    SYNC_STARTED = "sync_started"
    SYNC_COMPLETED = "sync_completed"
    SYNC_FAILED = "sync_failed"
    SCHEMA_DRIFT_DETECTED = "schema_drift_detected"
    QUALITY_CHECKS_FAILED = "quality_checks_failed"
    QUALITY_CHECKS_RECOVERED = "quality_checks_recovered"
    MODEL_REVIEWED = "model_reviewed"
    MODEL_IMPACTED = "model_impacted"
