"""Headwater exception hierarchy."""


class HeadwaterError(Exception):
    """Base exception for all Headwater errors."""


class ConfigError(HeadwaterError):
    """Configuration error (missing keys, invalid values)."""


class ConnectorError(HeadwaterError):
    """Error connecting to or reading from a data source."""


class ProfilerError(HeadwaterError):
    """Error during profiling or schema extraction."""


class AnalyzerError(HeadwaterError):
    """Error during LLM or heuristic analysis."""


class GeneratorError(HeadwaterError):
    """Error generating models or contracts."""


class ExecutorError(HeadwaterError):
    """Error executing models on the analytical engine."""


class MetadataError(HeadwaterError):
    """Error reading or writing the metadata store."""
