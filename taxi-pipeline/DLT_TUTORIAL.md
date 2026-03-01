# dlt Expert Tutorial: 80/20 Edition

**Goal:** Learn the 20% of dlt that gives you 80% of the results. Use this with the NYC taxi pipeline in this folder.

---

## Part 1: What is dlt? (2 min)

**dlt** = **d**ata **l**oad **t**ool. It’s a Python library that moves data from **sources** (APIs, DBs, files) into **destinations** (DuckDB, BigQuery, Snowflake, etc.) with minimal code.

- You define **where** data comes from and **where** it goes.
- dlt handles: typing, schema inference, normalization, loading, retries, and state.

**Why use it?**

| Without dlt | With dlt |
|-------------|----------|
| Write HTTP client, pagination, retries, schema, DB writes | Declare source + destination; run pipeline |
| Easy to get pagination or schema wrong | Built-in pagination, schema, and load semantics |

**Pareto takeaway:** dlt is “EL” (Extract + Load) with a bit of transform. You focus on defining sources; dlt does the rest.

---

## Part 2: The 5 Concepts That Matter (80% of What You Need)

These five ideas cover most day-to-day dlt usage.

### 1. Pipeline

A **pipeline** is the “runner”: it takes data from a source and loads it into a destination.

```python
import dlt

pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",   # unique name (used for state and dashboard)
    destination="duckdb",            # where data lands
    dataset_name="nyc_taxi_data",    # schema/database name in the destination
    progress="log",                  # optional: print progress
)
```

- **pipeline_name:** Used for state, logs, and the dlt dashboard. Keep it stable.
- **destination:** e.g. `duckdb`, `bigquery`, `snowflake`.
- **dataset_name:** Logical dataset (e.g. DuckDB schema name).

**In your project:** See `taxi_pipeline.py` lines 50–56: that’s your pipeline definition.

---

### 2. Source

A **source** is “where data comes from.” It’s a function decorated with `@dlt.source` that **yields resources** (or uses a helper that yields them).

```python
@dlt.source
def my_source():
    yield from rest_api_resources(config)   # or yield resource1; yield resource2
```

- One source can have multiple resources (e.g. `trips`, `drivers`).
- Each resource becomes one (or more) table(s) in the destination.

**In your project:** `taxi_pipeline_rest_api_source()` is the source; it yields one resource, `trips`.

---

### 3. Resource

A **resource** is one logical stream of data: one API endpoint, one table, one file. dlt loads each resource into table(s) in the destination.

- In the REST API source you define resources in the config; each entry in `resources` is one resource.
- Your pipeline has a single resource named `trips` → one main table (and possibly child tables if the data is nested).

**Pareto:** Think “one resource ≈ one table (or one set of related tables).”

---

### 4. Run: Extract → Normalize → Load

When you call `pipeline.run(source())`, dlt does three steps:

| Step | What happens |
|------|----------------|
| **Extract** | Your source runs; data is pulled (e.g. from the API) and written to temporary files. |
| **Normalize** | dlt infers/uses schema, flattens nested structures, and prepares load-ready data. |
| **Load** | Data is inserted into the destination (e.g. DuckDB). |

```python
load_info = pipeline.run(taxi_pipeline_rest_api_source())
```

- **Extract:** Paginated requests to the taxi API; each page is stored.
- **Normalize:** JSON is parsed, types inferred, nested objects (if any) turned into relations.
- **Load:** Rows are written to DuckDB.

You don’t implement these steps; you just define the source and run.

---

### 5. Destination & Dataset

- **Destination:** The system where data lives (e.g. DuckDB file, BigQuery project).
- **Dataset:** A logical grouping (e.g. DuckDB schema). Tables are created under this.

For DuckDB in your project:

- Destination: DuckDB (default file in `.dlt/` or as configured).
- Dataset: `nyc_taxi_data` → tables like `nyc_taxi_data.trips`.

**Pareto:** For local work, “DuckDB + dataset_name” is the 20% you need; cloud destinations are the next step.

---

## Part 3: Your Taxi Pipeline, Line by Line

We’ll map the 80/20 concepts to your actual code.

### Source and REST API config

```python
@dlt.source
def taxi_pipeline_rest_api_source(maximum_offset=None):
```

- `@dlt.source` marks this as a dlt source.
- The function will yield resources (here via `rest_api_resources`).

```python
config: RESTAPIConfig = {
    "client": {
        "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
    },
    "resources": [
        {
            "name": "trips",
            "endpoint": {
                "path": "data_engineering_zoomcamp_api",
                "data_selector": "$",
                "paginator": { ... },
            },
        },
    ],
}
yield from rest_api_resources(config)
```

- **client.base_url:** Base URL for all requests. Full URL = base_url + path.
- **resources:** List of endpoints. Each item = one resource (e.g. `trips`).
- **endpoint.path:** Path after base_url. So you’re calling `.../data_engineering_zoomcamp_api`.
- **data_selector:** JSONPath to the array in the response. `"$"` means “root” (the response is a single JSON array).
- **paginator:** Tells dlt how to request the next page (offset/limit here) and when to stop (`total_path: None`, `stop_after_empty_page: True`, optional `maximum_offset`).

**Pareto:** For REST APIs you’ll mostly tweak `base_url`, `path`, `data_selector`, and `paginator`; the rest is reuse.

### Pipeline and run

```python
pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi_data",
    progress="log",
)

if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)
```

- `pipeline.run(source())` runs Extract → Normalize → Load.
- `load_info` tells you what was loaded (which resources, row counts, etc.).

---

## Part 4: Hands-On: Run, Inspect, Query

### Run the pipeline

```bash
cd taxi-pipeline
source .venv/bin/activate
python taxi_pipeline.py
```

You should see progress and then a summary of loaded data.

### Inspect with the dlt dashboard

```bash
dlt pipeline taxi_pipeline show
```

This opens a UI where you can see:

- Load history
- Schema (tables, columns)
- Sample data

### Query with DuckDB

DuckDB stores data in a `.duckdb` file in the pipeline folder (e.g. `taxi_pipeline.duckdb`). The dataset is a **schema**: use the full table name **`nyc_taxi_data.trips`** in SQL (not just `trips`).

```python
import dlt

pipeline = dlt.attach("taxi_pipeline")  # attach to existing pipeline
with pipeline.destination_client() as client:
    with client.execute_query(
        "SELECT MIN(Trip_Pickup_DateTime) AS start_date, MAX(Trip_Pickup_DateTime) AS end_date FROM nyc_taxi_data.trips"
    ) as result:
        print(result.fetchall())
```

Or use the **dlt dataset interface**:

```python
import dlt

pipeline = dlt.attach("taxi_pipeline")
dataset = pipeline.dataset()
# Use dataset for queries, or export to pandas/ibis
```

**Helper script:** Run `python query_trips.py` to query from the command line. If you see a DuckDB "Conflicting lock" error, close any other DuckDB session (e.g. the CLI) first. See **Workarounds & Troubleshooting** for all workarounds.

**Pareto:** “Run script → `dlt pipeline show` → query in Python or SQL” is the loop that covers 80% of debugging and exploration.

---

## Part 5: The “Next 20%” to Get Even More Value

Once the basics are solid, these add a lot for relatively little effort.

### 1. dlt MCP Server (AI + metadata)

With the dlt MCP server, your IDE/AI can read pipeline metadata and docs.

- **Cursor:** Settings → Tools & MCP → add the dlt MCP server (see workshop README for the JSON).
- **Effect:** You can ask the AI things like “What tables are in taxi_pipeline?” or “Show schema for trips.”

### 2. Incremental loading

For APIs that support “give me data since X,” use **incremental** so you only fetch new rows.

```python
# Concept only; your taxi API may not support this
"incremental": {
    "cursor_path": "updated_at",
    "initial_value": "2024-01-01",
}
```

- **cursor_path:** Field used as cursor (e.g. timestamp or id).
- **initial_value:** Start from this value; next run continues from last seen value.

**Pareto:** One extra config block; big win for recurring pipelines.

### 3. Secrets and config

- **Secrets:** Put API keys in `.dlt/secrets.toml` (and add to `.gitignore`). Access with `dlt.secrets["key"]` or `dlt.secrets.value` in function params.
- **Config:** Use `.dlt/config.toml` for non-secret settings (e.g. log level, telemetry).

**Pareto:** Never commit keys; use secrets for anything sensitive.

### 4. Write disposition

Control how data is written:

- **replace:** Overwrite table each run (good for full refresh).
- **append:** Add rows (default for many sources).
- **merge:** Upsert by primary key.

You can set this in `resource_defaults` or per resource in the REST API config.

---

## Workarounds & Troubleshooting

Common issues and fixes when running the taxi pipeline and dlt locally.

### Pipeline runs for a long time

- **Cause:** The API is paginated (1,000 rows per page). A full load fetches until an empty page.
- **Quick test:** In `taxi_pipeline.py`, set `MAXIMUM_OFFSET = 5000` (or another number). The pipeline stops after that many rows (~1 min).
- **Full dataset (e.g. for homework):** Set `MAXIMUM_OFFSET = None` and run once; allow 10–30+ minutes.

### Dashboard: "You must install additional dependencies"

- **Cause:** The Workspace Dashboard needs `marimo`, `pyarrow`, and **ibis**. Sometimes `dlt[workspace]` doesn’t pull in ibis, or ibis fails to install on Python 3.9.
- **Fix:** Install the workspace extra and ibis with **pip** (not `python3 pip`):
  ```bash
  source .venv/bin/activate
  pip install "dlt[workspace]"
  pip install "ibis-framework[duckdb]"
  ```
- **Correct command:** Use `pip install ...` or `python -m pip install ...`. Do **not** run `python3 pip install ...` (Python tries to execute a file named `pip`).
- **If ibis won’t install** (e.g. on Python 3.9): Use the Python query options below instead of the dashboard.

### No DuckDB CLI installed

- **Cause:** `duckdb` command not found — the DuckDB CLI is separate from the Python package.
- **Options:**
  1. **Use Python only:** Run `python query_trips.py` (script in this folder), or use `dlt.attach("taxi_pipeline")` and `pipeline.destination_client()` as in Part 4.
  2. **Install DuckDB CLI:** e.g. `brew install duckdb` (macOS), then `duckdb taxi_pipeline.duckdb`.

### DuckDB: "Conflicting lock" / "Could not set lock on file"

- **Cause:** Only one process can open the same DuckDB file at a time (one writer/lock).
- **Fix:** Close the other process that has the DB open:
  - If you ran `duckdb taxi_pipeline.duckdb` in another terminal, type `.exit` or `quit` or press Ctrl+D in that terminal.
  - Then run your Python query or script again.

### Table name in queries

- **Cause:** Tables live under the **dataset** schema, not the default schema.
- **Use full name:** `nyc_taxi_data.trips`, not `trips`. Example: `SELECT * FROM nyc_taxi_data.trips LIMIT 5`.

### REST API: "Total items not found" / OffsetPaginator error

- **Cause:** The API returns a raw JSON array (no `total` key). The offset paginator expects either a total or an explicit “no total” hint.
- **Fix:** In the endpoint `paginator` config set `"total_path": None` and `"stop_after_empty_page": True` so pagination stops when a page is empty.

---

## Part 6: Cheat Sheet (80/20 in One Page)

| I want to… | Do this |
|------------|--------|
| Create a pipeline | `dlt.pipeline(pipeline_name=..., destination=..., dataset_name=...)` |
| Add a REST API source | Use `rest_api_source` or `rest_api_resources` with a `RESTAPIConfig` (client, resources, paginator). |
| Run the pipeline | `pipeline.run(my_source())` |
| Inspect loads | `dlt pipeline <name> show` |
| Query loaded data | `pipeline = dlt.attach("<name>"); pipeline.destination_client()` or `pipeline.dataset()` |
| Paginate an API | In endpoint config set `paginator` (e.g. `type: "offset"`, limit, offset_param, limit_param, stop_after_empty_page). |
| Use no total count | Set `total_path: None` and `stop_after_empty_page: True` (and optionally `maximum_offset`). |
| Limit rows (e.g. for testing) | Use `maximum_offset` in the offset paginator. |
| Keep API keys safe | Put them in `.dlt/secrets.toml` and reference via `dlt.secrets`. |
| Fix common issues | See **Workarounds & Troubleshooting** (dashboard, DuckDB lock, long run, table name). |

---

## Part 7: Mental Model Summary

1. **Pipeline** = runner (source → destination).
2. **Source** = function that yields **resources** (streams of data).
3. **Resource** = one logical stream → one (or more) table(s).
4. **Run** = Extract (fetch) → Normalize (schema, flatten) → Load (write to destination).
5. **Destination + dataset** = where and under which name data is stored.

**Pareto summary:** Learn pipeline, source, resource, run, and one destination (e.g. DuckDB). That’s the 20% that makes you effective. Add MCP, incremental, and secrets when you need them.

Use this file next to `taxi_pipeline.py`: change the config, run the pipeline, then inspect and query to see how each concept behaves in practice.
