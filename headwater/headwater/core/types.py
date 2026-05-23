"""Shared type aliases for cross-module contracts."""

from __future__ import annotations

from typing import Literal, TypeAlias

SuggestionSource: TypeAlias = Literal[
    "business",
    "mart",
    "relationship",
    "quality",
    "semantic",
    "statistical",
    "catalog",
    "cross_table",
]
