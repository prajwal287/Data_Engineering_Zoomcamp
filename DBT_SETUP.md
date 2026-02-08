# dbt Core + BigQuery Local Setup Guide

This guide walks you through installing dbt Core with the BigQuery adapter and testing it against your existing project. Each step explains **why** we do it.

---

## What is dbt?

**dbt (data build tool)** turns your SQL and YAML into:

- **Models**: SQL that runs against your warehouse (BigQuery). Models can reference each other with `{{ ref('model_name') }}` and raw tables with `{{ source('source_name', 'table_name') }}`.
- **Tests**: Assertions on your data (uniqueness, not null, relationships).
- **Documentation**: Generated from your YAML and model SQL.

Your project already has:

- **Staging models** (`stg_green_tripdata`, `stg_yellow_tripdata`) that read from BigQuery sources and clean column names/types.
- **Intermediate model** (`int_trips_unioned`) that unions green and yellow trips.
- **Marts** (e.g. `fct_trips`, `dim_vendors`, reporting) that build on top.

dbt will **compile** these `.sql` files (resolving `ref()` and `source()`) and **run** them in BigQuery in dependency order.

---

## Step 1: Use a virtual environment (already done)

We created `.venv` in the project root.

**Why?**  
Keeps dbt and its dependencies (BigQuery client, Jinja, etc.) isolated from your system Python and other projects. You avoid version clashes and can delete `.venv` to start fresh.

**Activate it** (run this in your terminal when working on dbt):

```bash
cd /Users/prajwalchambenandeeshappa/Data_Engineering_Zoomcamp
source .venv/bin/activate
```

You should see `(.venv)` in your prompt. All following commands assume you’re in this directory with the venv activated, or you use `.venv/bin/pip` and `.venv/bin/dbt`.

---

## Step 2: Install dbt Core + BigQuery adapter

From the project root (with venv activated):

```bash
pip install --upgrade pip wheel setuptools
pip install -r requirements-dbt.txt
```

**Why `requirements-dbt.txt`?**  
It pins `dbt-bigquery`, which automatically installs `dbt-core` and the Google Cloud libraries needed to talk to BigQuery. Pinning the version makes installs reproducible.

**Why upgrade pip/setuptools/wheel?**  
Newer pip resolves dependencies better and uses prebuilt wheels, which speeds up installs (especially for packages like `grpcio` and `cryptography`).

Install can take a few minutes. When it finishes, verify:

```bash
dbt --version
```

You should see something like: `Core: 1.11.x - BigQuery: 1.10.x`.

---

## Step 3: Configure the BigQuery connection (profiles)

dbt needs to know **how** to connect to BigQuery: project, dataset, and authentication. That’s in a **profile**.

**Where profiles live:**  
dbt looks for a file named `profiles.yml` in `~/.dbt/` (your home directory). It is **not** in the repo so you don’t commit credentials.

**What we created:**  
We added `profiles.yml.example` in the project. It matches your existing sources:

- **Profile name**: `default` (same as `profile: 'default'` in `dbt_project.yml`).
- **Project**: `psyched-loader-485321-a8` (from `models/staging/bigquery_sources.yml`).
- **Dataset**: `zoomcamp` (where dbt will create views/tables by default).

**What you do:**

1. **Option A – Use the home directory (recommended):**  
   Create the dbt config directory and copy the example:
   ```bash
   mkdir -p ~/.dbt
   cp profiles.yml.example ~/.dbt/profiles.yml
   ```
   Then run `dbt debug` and `dbt compile` without any extra env vars.

2. **Option B – Use a profile in the project (already set up):**  
   A `profiles.yml` was created in the project root (from the example). It’s in `.gitignore` so it won’t be committed. To use it, run dbt with:
   ```bash
   DBT_PROFILES_DIR=. dbt compile
   ```
   (Same for `dbt debug`, `dbt run`, etc.)

3. **Authentication**
   - **Option A – OAuth (good for local dev):**  
     In `~/.dbt/profiles.yml` keep `method: oauth`. The first time you run `dbt debug` or `dbt run`, a browser window will open for you to log in with your Google account. dbt will then use that to run queries in BigQuery.
   - **Option B – Service account (CI / automation):**  
     Create a GCP service account with BigQuery access, download a JSON key, then in `profiles.yml` set:
     ```yaml
     method: service_account
     keyfile: /path/to/your-service-account-key.json
     ```
     and keep `project` / `dataset` as in the example.

**Why `target: dev`?**  
You can have multiple targets (e.g. `dev`, `prod`) in the same profile. `dbt run` uses the one specified by `target` so you can point dev at a dev dataset and prod at a prod dataset.

---

## Step 4: Check the connection and project

From the project root:

```bash
dbt debug
```

**What it does:**  
Checks (1) that dbt can find your project and profile, (2) that it can connect to BigQuery with the credentials from `profiles.yml`, and (3) prints your project and connection details.

If you use OAuth, a browser may open for login the first time. Fix any errors (wrong project, missing dataset, invalid credentials) before continuing.

---

## Step 5: Compile (no BigQuery run)

```bash
dbt compile
```

**What it does:**  
Resolves all Jinja (`{{ ref(...) }}`, `{{ source(...) }}`, etc.) and writes the final SQL for each model into `target/`. It does **not** run anything in BigQuery. Use this to:

- Confirm that your SQL and refs/sources resolve correctly.
- Inspect the exact SQL that would run (e.g. open `target/compiled/.../stg_green_tripdata.sql`).

This is a good “syntax + project structure” check before running.

---

## Step 6: Run models in BigQuery

```bash
dbt run
```

**What it does:**  
Builds the dependency graph from your models, then runs each model’s SQL **in BigQuery** in order. By default, models are created as **views** (unless overridden in `dbt_project.yml` or in the model file with `config(materialized='table')`). So after `dbt run` you’ll see objects like:

- `zoomcamp.stg_green_tripdata`
- `zoomcamp.stg_yellow_tripdata`
- `zoomcamp.int_trips_unioned`
- etc.

**Prerequisite:**  
Your **sources** must exist in BigQuery: the `raw_data` source points to `psyched-loader-485321-a8.zoomcamp.green_tripdata` and `yellow_tripdata`. If those tables don’t exist yet, create them (e.g. from your ingestion pipeline or HW) or point the source in `models/staging/bigquery_sources.yml` to the correct project/dataset/table.

---

## Step 7: Run tests (optional but recommended)

```bash
dbt test
```

**What it does:**  
Runs tests defined in your YAML (e.g. uniqueness, not_null, relationships). If you’ve added tests to `schema.yml` or `bigquery_sources.yml`, they’ll run here. Good for catching bad data or broken refs after `dbt run`.

---

## Quick reference

| Command       | Purpose |
|--------------|---------|
| `dbt debug`  | Verify project + profile + BigQuery connection. |
| `dbt compile`| Resolve refs/sources and write SQL to `target/`. |
| `dbt run`    | Execute models in BigQuery (creates/updates views/tables). |
| `dbt test`   | Run data tests. |
| `dbt docs generate` then `dbt docs serve` | Generate and view project docs. |

---

## Project layout (reminder)

- **`dbt_project.yml`** – Project name, profile name, and model config (e.g. materialization).
- **`profiles.yml`** (in `~/.dbt/`) – Connection details (project, dataset, auth). Not in repo.
- **`models/staging/`** – Models that read from **sources** (raw BigQuery tables).
- **`models/staging/bigquery_sources.yml`** – Defines the `raw_data` source (project, dataset, table names).
- **`models/intermediate/`** – Models that depend on staging (e.g. union).
- **`models/marts/`** – Final reporting/dimensional models.

Once Step 2 (install) completes, run Steps 3–7 in order. If any step fails, the error message plus this guide should be enough to fix it.

---

## Troubleshooting

- **"Could not find profile named 'default'"**  
  dbt is not finding `profiles.yml`. Either copy `profiles.yml.example` to `~/.dbt/profiles.yml` (Option A above) or run with `DBT_PROFILES_DIR=.` from the project root (Option B).

- **"Failed to authenticate with supplied credentials"**  
  BigQuery needs credentials. With `method: oauth` in your profile, run once:
  ```bash
  gcloud auth application-default login
  ```
  A browser will open; sign in with the Google account that has access to your GCP project. After that, `dbt debug` and `dbt run` should work.

- **Python 3.9 warnings**  
  Your env may show warnings about Python 3.9 being past end-of-life. dbt still runs; for a cleaner experience, use Python 3.10+ in a new venv when you can.
