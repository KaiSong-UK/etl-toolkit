import pytest
from etl_toolkit import Pipeline, QualityCheck
from etl_toolkit.steps import StepResult, QualityCheck as QC


def test_quality_check_not_null_pass():
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    step = QC(name="check", rules=[{"type": "not_null", "columns": ["id", "name"]}], on_fail="error")
    result = step.execute({"_last_data": data})
    assert result.status == "success"


def test_quality_check_not_null_fail():
    data = [{"id": None, "name": "Alice"}]
    step = QC(name="check", rules=[{"type": "not_null", "columns": ["id"]}], on_fail="error")
    result = step.execute({"_last_data": data})
    assert result.status == "error"
    assert len(result.metrics["failures"]) == 1


def test_quality_check_not_null_warn():
    """Warn mode still succeeds the pipeline."""
    data = [{"id": None, "name": "Alice"}]
    pipeline = Pipeline("qc-warn").add_step(
        QC(name="check", rules=[{"type": "not_null", "columns": ["id"]}], on_fail="warn")
    )
    pipeline._context["_last_data"] = data
    result = pipeline.run()
    assert result.success is True


def test_quality_check_unique_pass():
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    step = QC(name="check", rules=[{"type": "unique", "columns": ["id"]}], on_fail="error")
    result = step.execute({"_last_data": data})
    assert result.status == "success"


def test_quality_check_unique_duplicate():
    """Duplicates must be caught across all rows, not just one."""
    data = [{"id": 1}, {"id": 2}, {"id": 1}]
    step = QC(name="check", rules=[{"type": "unique", "columns": ["id"]}], on_fail="error")
    result = step.execute({"_last_data": data})
    assert result.status == "error"
    assert len(result.metrics["failures"]) == 1


def test_quality_check_range_pass():
    data = [{"age": 25}, {"age": 35}]
    step = QC(name="check", rules=[{"type": "range", "column": "age", "min": 0, "max": 150}], on_fail="error")
    result = step.execute({"_last_data": data})
    assert result.status == "success"


def test_quality_check_range_fail():
    data = [{"age": 25}, {"age": 999}]
    step = QC(name="check", rules=[{"type": "range", "column": "age", "min": 0, "max": 150}], on_fail="error")
    result = step.execute({"_last_data": data})
    assert result.status == "error"
