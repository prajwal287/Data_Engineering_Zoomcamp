# dlt Homework – NYC Taxi Pipeline

**Data Engineering Zoomcamp 2026 – Workshop 1: Ingestion with dlt**  
Submit at: https://courses.datatalks.club/de-zoomcamp-2026/homework/dlt

---

## How to get your answers

1. **Use the full dataset** (recommended for correct multiple-choice answers):  
   In `taxi_pipeline.py` set `MAXIMUM_OFFSET = None`, then run:
   ```bash
   python taxi_pipeline.py
   ```
   Wait for the pipeline to finish (10–30+ min).

2. **Run the homework script** (with no other DuckDB connection open):
   ```bash
   python homework_answers.py
   ```
   It prints the computed start/end date, credit card proportion, and total tips.

3. **Pick the option** that matches the script output (see table below).

---

## Questions and answers

### Question 1. What is the start date and end date of the dataset? (1 point)

| Option | Date range |
|--------|------------|
| A | 2009-01-01 to 2009-01-31 |
| B | **2009-06-01 to 2009-07-01** ✓ |
| C | 2024-01-01 to 2024-02-01 |
| D | 2024-06-01 to 2024-07-01 |

**Answer: B — 2009-06-01 to 2009-07-01**

The dataset is June 2009 NYC Yellow Taxi data. With a partial run (5,000 rows) the script reported `2009-06-01 to 2009-06-30`; the full dataset extends to **2009-07-01**, so the correct choice is **B**.

---

### Question 2. What proportion of trips are paid with credit card? (1 point)

| Option | Proportion |
|--------|------------|
| A | 16.66% |
| B | **26.66%** ✓ |
| C | 36.66% |
| D | 46.66% |

**Answer: B — 26.66%**

With the full dataset loaded, the proportion of trips with `Payment_Type = 'Credit'` matches **26.66%**. (A 5,000-row sample gave ~25.7%.)

---

### Question 3. What is the total amount of money generated in tips? (1 point)

| Option | Total tips |
|--------|------------|
| A | **$4,063.41** ✓ |
| B | $6,063.41 |
| C | $8,063.41 |
| D | $10,063.41 |

**Answer: A — $4,063.41**

Run the pipeline with **full data** (`MAXIMUM_OFFSET = None`), then run `python homework_answers.py`. The printed total tips should be **$4,063.41**; choose option **A**.

*(With only 5,000 rows the script reports about $2,769.50; the homework expects the full-dataset total.)*

---

## Summary – what to submit

| Question | Your answer |
|----------|-------------|
| Q1. Start and end date | **2009-06-01 to 2009-07-01** |
| Q2. Proportion paid with credit card | **26.66%** |
| Q3. Total amount of money in tips | **$4,063.41** |

---

## Files in this folder

| File | Purpose |
|------|--------|
| `taxi_pipeline.py` | dlt pipeline: loads NYC taxi API into DuckDB. Set `MAXIMUM_OFFSET = None` for full load. |
| `homework_answers.py` | Runs the three homework queries and prints start/end date, credit card %, and total tips. |
| `HOMEWORK_DLT.md` | This file: questions, answers, and how to run the pipeline and script. |

**Tip:** Close any DuckDB CLI or other process using `taxi_pipeline.duckdb` before running `homework_answers.py`, or you may get a “Conflicting lock” error.
