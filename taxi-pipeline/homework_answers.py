#!/usr/bin/env python3
"""
dlt Homework: Answer the three questions by querying the loaded NYC taxi data.
Run from taxi-pipeline folder with venv activated: python homework_answers.py

Ensure no other process has the DuckDB file open (e.g. close the duckdb CLI).
For correct multiple-choice answers, run the pipeline with MAXIMUM_OFFSET = None first (full dataset).
"""

import dlt


def run_queries():
    p = dlt.attach("taxi_pipeline")
    with p.sql_client() as client:
        # Q1: Start and end date of the dataset
        q1 = '''
            SELECT 
                MIN(trip_pickup_date_time)::DATE AS start_date,
                MAX(trip_pickup_date_time)::DATE AS end_date
            FROM nyc_taxi_data.trips
        '''
        with client.execute_query(q1) as result:
            row = result.fetchone()
            start_date, end_date = row[0], row[1]
            print("Q1. Start date and end date of the dataset:")
            print(f"    {start_date} to {end_date}")
            print()

        # Q2: Proportion of trips paid with credit card
        q2 = '''
            SELECT 
                COUNT(*) AS total_trips,
                COUNT(*) FILTER (WHERE payment_type = 'Credit') AS credit_trips,
                ROUND(100.0 * COUNT(*) FILTER (WHERE payment_type = 'Credit') / COUNT(*), 2) AS pct_credit
            FROM nyc_taxi_data.trips
        '''
        with client.execute_query(q2) as result:
            row = result.fetchone()
            total, credit, pct = row[0], row[1], row[2]
            print("Q2. Proportion of trips paid with credit card:")
            print(f"    {credit} / {total} = {pct}%")
            print()

        # Q3: Total amount of money generated in tips
        q3 = '''
            SELECT ROUND(SUM(tip_amt)::NUMERIC, 2) AS total_tips
            FROM nyc_taxi_data.trips
        '''
        with client.execute_query(q3) as result:
            total_tips = result.fetchone()[0]
            print("Q3. Total amount of money generated in tips:")
            print(f"    ${total_tips:,.2f}")
            print()

        return {
            "q1": {"start_date": str(start_date), "end_date": str(end_date)},
            "q2": {"pct_credit": float(pct), "total_trips": total, "credit_trips": credit},
            "q3": {"total_tips": total_tips},
        }


if __name__ == "__main__":
    run_queries()
