#!/usr/bin/env python3
"""
ETL Toolkit Demo — No database required!
Uses SQLite in-memory to demonstrate a complete data pipeline.
Run: python examples/demo.py
"""
import sqlite3
from etl_toolkit import Pipeline, Transform, QualityCheck, LogStep
from etl_toolkit.steps import WriteDatabase, ReadDatabase


def setup_db(conn):
    """Create demo tables and seed data."""
    cur = conn.cursor()

    # Source table: raw orders from POS
    cur.execute("""
        CREATE TABLE IF NOT EXISTS src_orders (
            order_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            email TEXT,
            product TEXT,
            quantity INTEGER,
            unit_price REAL,
            created_at TEXT
        )
    """)

    # Target warehouse table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS warehouse_orders (
            order_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            email TEXT,
            product TEXT,
            quantity INTEGER,
            unit_price REAL,
            total_amount REAL,
            created_at TEXT,
            synced_at TEXT
        )
    """)

    # Clear old data
    cur.execute("DELETE FROM src_orders")
    cur.execute("DELETE FROM warehouse_orders")

    # Seed sample data
    import datetime
    base = datetime.date(2026, 4, 10)
    orders = [
        (1, "Alice Johnson", "alice@example.com", "DataQGuard Pro", 1, 299.00, str(base - datetime.timedelta(days=3))),
        (2, "Bob Chen", "bob.chen@example.com", "AI Analyst License", 3, 199.00, str(base - datetime.timedelta(days=2))),
        (3, "Carol Davis", "carol@example.com", "ETL Toolkit", 1, 399.00, str(base - datetime.timedelta(days=1))),
        (4, "David Kim", "david@example.com", "CloudSync Pro", 5, 99.00, str(base)),
        (5, "Emma Wilson", "emma@example.com", "DataQGuard Pro", 1, 299.00, str(base + datetime.timedelta(days=1))),
    ]
    cur.executemany(
        "INSERT INTO src_orders VALUES (?, ?, ?, ?, ?, ?, ?)",
        orders
    )
    conn.commit()
    print(f"  ✓ Seeded {len(orders)} orders into src_orders")


def main():
    print("\n" + "=" * 60)
    print("  ETL Toolkit Demo — In-Memory SQLite Pipeline")
    print("=" * 60)

    # ── Setup in-memory SQLite ─────────────────────────────────────
    conn = sqlite3.connect(":memory:")
    setup_db(conn)
    conn_uri = "file::memory:?cache=shared"
    real_conn = sqlite3.connect(":memory:", uri=True)
    real_connuri = "file::memory:?cache=shared&uri=true"
    # Use a temp file DB for Write step (in-memory can't be shared across connections)
    import tempfile, os
    tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp_db.close()
    warehouse_conn = f"file:{tmp_db.name}?mode=rwc&cache=shared"

    # Re-create tables in warehouse DB
    wc = sqlite3.connect(tmp_db.name)
    wc.execute("""
        CREATE TABLE IF NOT EXISTS warehouse_orders (
            order_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            email TEXT,
            product TEXT,
            quantity INTEGER,
            unit_price REAL,
            total_amount REAL,
            created_at TEXT,
            synced_at TEXT
        )
    """)
    wc.commit()
    wc.close()

    # ── Pipeline Definition ────────────────────────────────────────
    pipeline = Pipeline("daily_order_sync", description="Sync POS orders to warehouse")

    # Step 1: Read from source
    pipeline.add_step(ReadDatabase(
        name="read_orders",
        connection=warehouse_conn,   # read from source for demo (same tmp db)
        query="SELECT * FROM src_orders",
    ))

    # Step 2: Quality checks
    pipeline.add_step(QualityCheck(
        name="qc_orders",
        rules=[
            {"type": "not_null", "columns": ["order_id", "customer_name", "email"]},
            {"type": "unique",   "columns": ["order_id"]},
            {"type": "range",    "column": "quantity", "min": 1, "max": 100},
            {"type": "range",    "column": "unit_price", "min": 0.01},
        ],
        on_fail="error",
    ))

    # Step 3: Transform — normalize emails, compute total
    pipeline.add_step(Transform(
        name="enrich_orders",
        fn=lambda rows: [
            {
                **r,
                "email": r["email"].lower().strip(),
                "total_amount": round(r["quantity"] * r["unit_price"], 2),
                "synced_at": str(__import__("datetime").date.today()),
            }
            for r in rows
        ],
    ))

    # Step 4: Write to warehouse
    pipeline.add_step(WriteDatabase(
        name="write_warehouse",
        connection=warehouse_conn,
        table="warehouse_orders",
        mode="upsert",
        key_columns=["order_id"],
    ))

    # Step 5: Log results
    pipeline.add_step(LogStep("log_summary"))

    # ── Run ───────────────────────────────────────────────────────
    print("\n🚀 Running pipeline...\n")
    result = pipeline.run()

    # ── Report ────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    if result.success:
        print("  ✅ Pipeline SUCCEEDED")
    else:
        print("  ❌ Pipeline FAILED")
        print(f"     {result.error}")

    print(f"\n  ⏱  Total duration: {result.total_duration_ms:.1f}ms")
    print(f"  📦 Rows written:   {result.rows_written}")

    print("\n  📋 Step Results:")
    for sr in result.step_results:
        icon = "✅" if sr.status == "success" else "❌"
        print(f"     {icon} [{sr.status.upper():7}] {sr.step_name}")
        print(f"              rows={sr.rows_read}/{sr.rows_written}  {sr.duration_ms:.1f}ms")

    # Cleanup
    conn.close()
    os.unlink(tmp_db.name)

    print("\n" + "=" * 60)
    return result


if __name__ == "__main__":
    main()
