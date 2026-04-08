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
        assert "Demo complete" in result.output

    def test_demo_bad_path(self):
        result = runner.invoke(app, ["demo", "--dataset", "/nonexistent/path"])
        assert result.exit_code == 1


class TestCLIDiscover:
    def test_discover(self):
        result = runner.invoke(app, ["discover-cmd", SAMPLE_DATA])
        assert result.exit_code == 0
        assert "zones" in result.output

    def test_discover_bad_path(self):
        result = runner.invoke(app, ["discover-cmd", "/nonexistent"])
        assert result.exit_code == 1


class TestCLIGenerate:
    def test_generate(self):
        result = runner.invoke(app, ["generate", SAMPLE_DATA])
        assert result.exit_code == 0
        assert "staging" in result.output.lower()
        assert "contracts" in result.output.lower()

    def test_generate_bad_path(self):
        result = runner.invoke(app, ["generate", "/nonexistent"])
        assert result.exit_code == 1
