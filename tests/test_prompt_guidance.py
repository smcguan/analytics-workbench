"""
test_prompt_guidance.py — verify AI prompt content includes critical syntax guidance

Covers:
  build_insights_prompt() — DuckDB percentile function guidance
  build_sql_prompt() — DuckDB percentile function guidance

Run from project root:
    pytest tests/test_prompt_guidance.py -v
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Fixture: tiny Parquet dataset for prompt builders
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def tiny_dataset_dir(tmp_path_factory) -> Path:
    tmp = tmp_path_factory.mktemp("prompt_guidance")
    df = pd.DataFrame({
        "drug_name": ["DrugA", "DrugB", "DrugC"],
        "total_paid": [100.0, 200.0, 300.0],
    })
    df.to_parquet(str(tmp / "source.parquet"), index=False)
    return tmp


def _source_path_fn(name: str, *, base: Path):
    return str(base / "source.parquet"), False


# ---------------------------------------------------------------------------
# Tests: insights prompt contains percentile guidance
# ---------------------------------------------------------------------------
class TestInsightsPromptPercentileGuidance:
    def test_warns_against_approx_percentile_cont(self, tiny_dataset_dir):
        from app.ai.provider_openai import build_insights_prompt

        prompt = build_insights_prompt(
            dataset_name="test",
            dataset_source_path_fn=lambda n: _source_path_fn(n, base=tiny_dataset_dir),
        )
        assert "APPROX_PERCENTILE_CONT" in prompt
        assert "does not exist" in prompt.lower() or "do not use" in prompt.upper().lower()

    def test_includes_percentile_cont_syntax(self, tiny_dataset_dir):
        from app.ai.provider_openai import build_insights_prompt

        prompt = build_insights_prompt(
            dataset_name="test",
            dataset_source_path_fn=lambda n: _source_path_fn(n, base=tiny_dataset_dir),
        )
        assert "PERCENTILE_CONT" in prompt
        assert "WITHIN GROUP" in prompt

    def test_includes_quantile_cont_alternative(self, tiny_dataset_dir):
        from app.ai.provider_openai import build_insights_prompt

        prompt = build_insights_prompt(
            dataset_name="test",
            dataset_source_path_fn=lambda n: _source_path_fn(n, base=tiny_dataset_dir),
        )
        assert "QUANTILE_CONT" in prompt

    def test_includes_median_shorthand(self, tiny_dataset_dir):
        from app.ai.provider_openai import build_insights_prompt

        prompt = build_insights_prompt(
            dataset_name="test",
            dataset_source_path_fn=lambda n: _source_path_fn(n, base=tiny_dataset_dir),
        )
        assert "MEDIAN" in prompt


# ---------------------------------------------------------------------------
# Tests: SQL generation prompt contains percentile guidance
# ---------------------------------------------------------------------------
class TestSQLPromptPercentileGuidance:
    def test_warns_against_approx_percentile_cont(self, tiny_dataset_dir):
        from app.ai.provider_openai import build_sql_prompt

        prompt = build_sql_prompt(
            dataset_name="test",
            question="What is the median spending?",
            dataset_source_path_fn=lambda n: _source_path_fn(n, base=tiny_dataset_dir),
        )
        assert "APPROX_PERCENTILE_CONT" in prompt
        assert "does not exist" in prompt.lower() or "do not use" in prompt.upper().lower()

    def test_includes_percentile_cont_syntax(self, tiny_dataset_dir):
        from app.ai.provider_openai import build_sql_prompt

        prompt = build_sql_prompt(
            dataset_name="test",
            question="What is the median spending?",
            dataset_source_path_fn=lambda n: _source_path_fn(n, base=tiny_dataset_dir),
        )
        assert "PERCENTILE_CONT" in prompt
        assert "WITHIN GROUP" in prompt

    def test_includes_quantile_cont_alternative(self, tiny_dataset_dir):
        from app.ai.provider_openai import build_sql_prompt

        prompt = build_sql_prompt(
            dataset_name="test",
            question="What is the median spending?",
            dataset_source_path_fn=lambda n: _source_path_fn(n, base=tiny_dataset_dir),
        )
        assert "QUANTILE_CONT" in prompt

    def test_includes_median_shorthand(self, tiny_dataset_dir):
        from app.ai.provider_openai import build_sql_prompt

        prompt = build_sql_prompt(
            dataset_name="test",
            question="What is the median spending?",
            dataset_source_path_fn=lambda n: _source_path_fn(n, base=tiny_dataset_dir),
        )
        assert "MEDIAN" in prompt
