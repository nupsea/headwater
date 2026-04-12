"""Tests for the CLI layer."""

from __future__ import annotations

from typer.testing import CliRunner

from headwater.cli.main import app

runner = CliRunner()

SAMPLE_DATA = str(
    __import__("pathlib").Path(__file__).resolve().parent.parent.parent
    / "data"
    / "sample"
)


class TestCLIBasic:
    def test_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Headwater" in result.output

    def test_version(self):
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert "headwater" in result.output

    def test_status(self):
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        assert "LLM provider" in result.output


class TestCLIDemo:
    def test_demo_with_sample_data(self):
        result = runner.invoke(app, ["demo", "--dataset", SAMPLE_DATA])
        assert result.exit_code == 0
        assert "Loaded" in result.output
        assert "Demo Complete" in result.output or "Demo complete" in result.output
        # US-701: verify "What happened" summary and next-step instructions
        assert "Next steps" in result.output or "next steps" in result.output

    def test_demo_bad_path(self):
        result = runner.invoke(app, ["demo", "--dataset", "/nonexistent/path"])
        assert result.exit_code == 1


class TestCLIDiscover:
    def test_discover(self):
        result = runner.invoke(app, ["discover", "--source", SAMPLE_DATA])
        assert result.exit_code == 0
        assert "zones" in result.output

    def test_discover_bad_path(self):
        result = runner.invoke(app, ["discover", "--source", "/nonexistent"])
        assert result.exit_code == 1

    def test_discover_with_explicit_type(self):
        result = runner.invoke(app, ["discover", "--source", SAMPLE_DATA, "--type", "json"])
        assert result.exit_code == 0
        assert "zones" in result.output

    def test_discover_with_name(self):
        result = runner.invoke(app, ["discover", "--source", SAMPLE_DATA, "--name", "mydata"])
        assert result.exit_code == 0

    def test_discover_observe_mode_not_implemented(self):
        result = runner.invoke(app, ["discover", "--source", SAMPLE_DATA, "--mode", "observe"])
        assert result.exit_code == 1
        assert "not yet implemented" in result.output.lower()


class TestCLIGenerate:
    def test_generate(self):
        result = runner.invoke(app, ["generate", SAMPLE_DATA])
        assert result.exit_code == 0
        assert "staging" in result.output.lower()
        assert "contracts" in result.output.lower()

    def test_generate_bad_path(self):
        result = runner.invoke(app, ["generate", "/nonexistent"])
        assert result.exit_code == 1
