import time
import logging
from typing import Any, Optional
from dataclasses import dataclass, field
from .steps import PipelineStep, StepResult

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    pipeline_name: str
    success: bool
    total_duration_ms: float
    step_results: list[StepResult]
    rows_written: int = 0
    error: Optional[str] = None


class Pipeline:
    """Define and run ETL pipelines with built-in observability."""

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.steps: list[PipelineStep] = []
        self._context: dict[str, Any] = {}

    def add_step(self, step: PipelineStep) -> "Pipeline":
        self.steps.append(step)
        return self

    def run(self) -> PipelineResult:
        """Execute all steps in order. Returns a PipelineResult."""
        start = time.time()
        step_results: list[StepResult] = []
        total_rows_written = 0

        logger.info(f"🚀 Starting pipeline: {self.name}")

        for step in self.steps:
            step_name = step.name
            step_start = time.time()
            logger.info(f"  → Step: {step_name}")

            try:
                result = step.execute(self._context)
                step_duration = (time.time() - step_start) * 1000

                if result.rows_written:
                    total_rows_written += result.rows_written

                if result.status == "error":
                    logger.error(f"  ✗ Step '{step_name}' failed: {result.error}")
                    return PipelineResult(
                        pipeline_name=self.name,
                        success=False,
                        total_duration_ms=(time.time() - start) * 1000,
                        step_results=step_results,
                        error=f"Step '{step_name}' failed: {result.error}",
                    )

                logger.info(
                    f"  ✓ Step '{step_name}' done in {step_duration:.1f}ms, "
                    f"rows={result.rows_read}/{result.rows_written}"
                )
                step_results.append(result)

            except Exception as e:
                logger.exception(f"  ✗ Step '{step_name}' raised: {e}")
                return PipelineResult(
                    pipeline_name=self.name,
                    success=False,
                    total_duration_ms=(time.time() - start) * 1000,
                    step_results=step_results,
                    error=str(e),
                )

        total_ms = (time.time() - start) * 1000
        logger.info(
            f"✅ Pipeline '{self.name}' completed in {total_ms:.1f}ms, "
            f"wrote {total_rows_written} rows"
        )
        return PipelineResult(
            pipeline_name=self.name,
            success=True,
            total_duration_ms=total_ms,
            step_results=step_results,
            rows_written=total_rows_written,
        )
