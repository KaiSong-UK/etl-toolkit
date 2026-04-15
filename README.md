<div align="center">

# 🔧 ETL Toolkit

### Lightweight, Observable ETL Framework for Python

[![CI](https://github.com/KaiSong-UK/etl-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/KaiSong-UK/etl-toolkit/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI](https://img.shields.io/pypi/v/etl-toolkit)](https://pypi.org/project/etl-toolkit/)
[![Python](https://img.shields.io/pypi/pyversions/etl-toolkit)](https://pypi.org/project/etl-toolkit/)

**Build reliable data pipelines with built-in quality checks**

</div>

---

## ✨ Features

- ✅ **Built-in Data Quality Checks** — Validate at every pipeline step
- 🔄 **Automatic Retry & Error Handling** — Configurable retry strategies with exponential backoff
- 📊 **Pipeline Observability** — Track execution status, duration, row counts, and errors
- 🔌 **Connector System** — PostgreSQL, MySQL, S3, CSV, JSON out of the box
- 📝 **YAML Pipeline Definition** — Define pipelines declaratively or in Python
- 🚀 **Lightweight** — No heavy framework overhead, runs anywhere Python runs

## 🚀 Quick Start

```bash
pip install etl-toolkit
```

```python
from etl_toolkit import Pipeline, steps

pipeline = Pipeline("daily_user_sync")

pipeline.add_step(steps.ReadDatabase(
    name="read_users",
    connection="postgresql://user:pass@host/db",
    query="SELECT * FROM users WHERE updated_at > :last_run",
    params={"last_run": "${LAST_RUN}"}
))

pipeline.add_step(steps.QualityCheck(
    name="check_completeness",
    rules=[
        {"type": "not_null", "columns": ["user_id", "email"]},
        {"type": "unique",   "columns": ["user_id"]},
        {"type": "range",    "column": "age", "min": 0, "max": 150}
    ],
    on_fail="warn"
))

pipeline.add_step(steps.Transform(
    name="normalize_email",
    fn=lambda rows: [{**r, "email": r["email"].lower().strip()} for r in rows]
))

pipeline.add_step(steps.WriteDatabase(
    name="write_warehouse",
    connection="postgresql://user:pass@warehouse/db",
    table="dim_users",
    mode="upsert",
    key_columns=["user_id"]
))

result = pipeline.run()
print(f"✅ Synced {result.rows_written} users in {result.duration:.1f}s")
```

## 🏃 Run the Demo

```bash
git clone https://github.com/KaiSong-UK/etl-toolkit.git
cd etl-toolkit
pip install -e .
python examples/demo.py
```

No database required — uses in-memory SQLite.

## 🔧 Step Types

| Step | Description |
|------|-------------|
| `ReadDatabase` | Read from PostgreSQL / MySQL |
| `ReadCSV` | Load CSV files |
| `Transform` | Apply a Python function |
| `QualityCheck` | Validate: not_null, unique, range |
| `WriteDatabase` | Write to DB (append / upsert) |
| `WriteCSV` | Export to CSV |
| `LogStep` | Log without modifying data |

## 🧪 Testing

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

## 📄 License

MIT
