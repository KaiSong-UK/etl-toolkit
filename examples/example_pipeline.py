"""
Example: Daily User Sync Pipeline
"""
import os
from etl_toolkit import Pipeline, ReadDatabase, WriteDatabase, QualityCheck, Transform

pipeline = Pipeline("daily_user_sync")

pipeline.add_step(
    ReadDatabase(
        name="read_users",
        connection=os.environ["DB_URL"],
        query="SELECT * FROM users WHERE updated_at > :last_run",
        params={"last_run": os.environ.get("LAST_RUN", "1970-01-01")},
    )
)

pipeline.add_step(
    QualityCheck(
        name="check_completeness",
        rules=[
            {"type": "not_null", "columns": ["user_id", "email"]},
            {"type": "unique", "columns": ["user_id"]},
            {"type": "range", "column": "age", "min": 0, "max": 150},
        ],
        on_fail="warn",
    )
)

pipeline.add_step(
    Transform(
        name="normalize_email",
        fn=lambda rows: [
            {**r, "email": r["email"].lower().strip()} for r in rows
        ],
    )
)

pipeline.add_step(
    WriteDatabase(
        name="write_warehouse",
        connection=os.environ["WAREHOUSE_URL"],
        table="dim_users",
        mode="upsert",
        key_columns=["user_id"],
    )
)

if __name__ == "__main__":
    result = pipeline.run()
    print(f"\n{'✅' if result.success else '❌'} Pipeline finished in {result.total_duration_ms:.1f}ms")
    print(f"   Rows written: {result.rows_written}")
    if result.error:
        print(f"   Error: {result.error}")
