from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional
import re


@dataclass
class StepResult:
    step_name: str
    status: str  # "success" | "error" | "skipped" | "warn"
    rows_read: int = 0
    rows_written: int = 0
    duration_ms: float = 0.0
    error: Optional[str] = None
    metrics: dict[str, Any] = field(default_factory=dict)


class PipelineStep(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def execute(self, ctx: dict[str, Any]) -> StepResult:
        raise NotImplementedError


# ──────────────────────────────────────────────────────────────────────────────
# Source Steps
# ──────────────────────────────────────────────────────────────────────────────


class ReadDatabase(PipelineStep):
    """Read rows from a SQL database."""

    def __init__(
        self,
        name: str,
        connection: str,
        query: str,
        params: Optional[dict] = None,
        batch_size: int = 10000,
    ):
        super().__init__(name)
        self.connection = connection
        self.query = query
        self.params = params or {}
        self.batch_size = batch_size

    def execute(self, ctx: dict[str, Any]) -> StepResult:
        import time
        start = time.time()

        try:
            import psycopg2
        except ImportError:
            try:
                import pymysql as psycopg2  # type: ignore
            except ImportError:
                return StepResult(
                    self.name, "error",
                    error="Neither psycopg2 nor pymysql is installed"
                )

        try:
            conn = psycopg2.connect(self.connection)
            cur = conn.cursor()
            cur.execute(self.query, self.params)

            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            data = [dict(zip(cols, r)) for r in rows]

            ctx["_last_data"] = data
            ctx["_last_batch"] = data  # alias for compat

            cur.close()
            conn.close()

            return StepResult(
                self.name, "success",
                rows_read=len(data),
                duration_ms=(time.time() - start) * 1000,
            )
        except Exception as e:
            return StepResult(self.name, "error", error=str(e))


class ReadCSV(PipelineStep):
    """Read a CSV file into memory."""

    def __init__(self, name: str, path: str, **kwargs):
        super().__init__(name)
        self.path = path
        self.csv_kwargs = kwargs

    def execute(self, ctx: dict[str, Any]) -> StepResult:
        import time
        start = time.time()
        try:
            import pandas as pd
            df = pd.read_csv(self.path, **self.csv_kwargs)
            data = df.to_dict(orient="records")
            ctx["_last_data"] = data
            return StepResult(self.name, "success", rows_read=len(data), duration_ms=(time.time() - start) * 1000)
        except Exception as e:
            return StepResult(self.name, "error", error=str(e))


# ──────────────────────────────────────────────────────────────────────────────
# Transform Steps
# ──────────────────────────────────────────────────────────────────────────────


class Transform(PipelineStep):
    """Apply a Python transform function to the data in context."""

    def __init__(self, name: str, fn: callable):
        super().__init__(name)
        self.fn = fn

    def execute(self, ctx: dict[str, Any]) -> StepResult:
        import time
        start = time.time()
        data = ctx.get("_last_data", [])
        try:
            result = self.fn(data)
            ctx["_last_data"] = result
            return StepResult(self.name, "success", rows_read=len(data), rows_written=len(result), duration_ms=(time.time() - start) * 1000)
        except Exception as e:
            return StepResult(self.name, "error", error=str(e))


# ──────────────────────────────────────────────────────────────────────────────
# Quality Check Step
# ──────────────────────────────────────────────────────────────────────────────


class QualityCheck(PipelineStep):
    """Validate data quality rules. Supports: not_null, unique, range, enum."""

    def __init__(self, name: str, rules: list[dict], on_fail: str = "error"):
        super().__init__(name)
        self.rules = rules
        self.on_fail = on_fail  # "error" | "warn" | "skip"

    def execute(self, ctx: dict[str, Any]) -> StepResult:
        import time
        start = time.time()
        data = ctx.get("_last_data", [])
        failures: list[dict] = []

        for row in data:
            for rule in self.rules:
                rtype = rule["type"]
                cols = rule.get("columns", [rule.get("column")])
                for col in cols:
                    val = row.get(col)
                    if rtype == "not_null" and val is None:
                        failures.append({"rule": "not_null", "column": col, "row": row})
                    elif rtype == "unique":
                        seen = set()
                        for r in data:
                            v = r.get(col)
                            if v in seen:
                                failures.append({"rule": "unique", "column": col, "value": v})
                            seen.add(v)
                    elif rtype == "range" and val is not None:
                        lo = rule.get("min")
                        hi = rule.get("max")
                        if lo is not None and val < lo:
                            failures.append({"rule": "range", "column": col, "val": val, "min": lo})
                        if hi is not None and val > hi:
                            failures.append({"rule": "range", "column": col, "val": val, "max": hi})

        status = "success"
        if failures:
            status = self.on_fail

        return StepResult(
            self.name, status,
            rows_read=len(data),
            rows_written=len(data) if status != "error" else 0,
            duration_ms=(time.time() - start) * 1000,
            metrics={"failures": failures[:100]},
            error=f"{len(failures)} quality failures" if failures else None,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Sink Steps
# ──────────────────────────────────────────────────────────────────────────────


class WriteDatabase(PipelineStep):
    """Write rows to a database table. Supports 'append' and 'upsert' modes."""

    def __init__(
        self,
        name: str,
        connection: str,
        table: str,
        mode: str = "append",
        key_columns: Optional[list] = None,
        batch_size: int = 1000,
    ):
        super().__init__(name)
        self.connection = connection
        self.table = table
        self.mode = mode
        self.key_columns = key_columns or []
        self.batch_size = batch_size

    def execute(self, ctx: dict[str, Any]) -> StepResult:
        import time
        start = time.time()
        data = ctx.get("_last_data", [])

        try:
            import psycopg2
        except ImportError:
            try:
                import pymysql as psycopg2  # type: ignore
            except ImportError:
                return StepResult(self.name, "error", error="No DB driver installed")

        try:
            conn = psycopg2.connect(self.connection)
            cur = conn.cursor()

            if not data:
                return StepResult(self.name, "success", duration_ms=(time.time() - start) * 1000)

            cols = list(data[0].keys())
            placeholders = ", ".join(["%s"] * len(cols))
            col_names = ", ".join(cols)

            if self.mode == "upsert" and self.key_columns:
                upsert = f"""
                    INSERT INTO {self.table} ({col_names})
                    VALUES ({placeholders})
                    ON CONFLICT ({", ".join(self.key_columns)})
                    DO UPDATE SET {
                        ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in self.key_columns)
                    }
                """
            else:
                upsert = f"INSERT INTO {self.table} ({col_names}) VALUES ({placeholders})"

            total = 0
            for i in range(0, len(data), self.batch_size):
                batch = data[i : i + self.batch_size]
                cur.executemany(upsert, [tuple(r.get(c) for c in cols) for r in batch])
                total += len(batch)

            conn.commit()
            cur.close()
            conn.close()

            return StepResult(self.name, "success", rows_written=total, duration_ms=(time.time() - start) * 1000)
        except Exception as e:
            return StepResult(self.name, "error", error=str(e))


class WriteCSV(PipelineStep):
    """Write data to a CSV file."""

    def __init__(self, name: str, path: str, **kwargs):
        super().__init__(name)
        self.path = path
        self.csv_kwargs = kwargs

    def execute(self, ctx: dict[str, Any]) -> StepResult:
        import time
        start = time.time()
        try:
            import pandas as pd
            data = ctx.get("_last_data", [])
            df = pd.DataFrame(data)
            df.to_csv(self.path, index=False, **self.csv_kwargs)
            return StepResult(self.name, "success", rows_written=len(data), duration_ms=(time.time() - start) * 1000)
        except Exception as e:
            return StepResult(self.name, "error", error=str(e))


class LogStep(PipelineStep):
    """Log step results without modifying data."""

    def __init__(self, name: str = "log"):
        super().__init__(name)

    def execute(self, ctx: dict[str, Any]) -> StepResult:
        import time
        start = time.time()
        data = ctx.get("_last_data", [])
        print(f"[LogStep] Rows in context: {len(data)}")
        return StepResult(self.name, "success", rows_read=len(data), duration_ms=(time.time() - start) * 1000)
