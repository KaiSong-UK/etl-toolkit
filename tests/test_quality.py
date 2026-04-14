import pytest
from etl_toolkit import Pipeline, QualityCheck


def test_quality_check_pass():
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    pipeline = (
        Pipeline("qc-pass")
        .add_step(QualityCheck(
            name="check",
            rules=[
                {"type": "not_null", "columns": ["id", "name"]},
            ],
            on_fail="error",
        ))
    )
    pipeline._context["_last_data"] = data
    result = pipeline.run()
    assert result.success is True


def test_quality_check_fail_warn():
    data = [{"id": None, "name": "Alice"}]

    pipeline = Pipeline("qc-warn").add_step(
        QualityCheck(name="check", rules=[{"type": "not_null", "columns": ["id"]}], on_fail="warn")
    )
    pipeline._context["_last_data"] = data
    result = pipeline.run()
    # warn still succeeds pipeline
    assert result.success is True
