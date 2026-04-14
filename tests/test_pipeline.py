import pytest
from etl_toolkit import Pipeline, Transform, LogStep


def test_pipeline_success():
    pipeline = (
        Pipeline("test-pipeline")
        .add_step(Transform("double", fn=lambda rows: [{"id": r["id"] * 2} for r in rows]))
        .add_step(LogStep())
    )
    result = pipeline.run()
    assert result.success is True
    assert len(result.step_results) == 2


def test_pipeline_no_steps():
    pipeline = Pipeline("empty")
    result = pipeline.run()
    assert result.success is True
    assert result.total_duration_ms >= 0
