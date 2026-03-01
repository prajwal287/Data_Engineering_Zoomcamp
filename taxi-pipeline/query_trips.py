#!/usr/bin/env python3
"""
Query the NYC taxi data loaded by the dlt pipeline.
Run from taxi-pipeline folder with venv activated: python query_trips.py
"""
import dlt

def main():
    p = dlt.attach("taxi_pipeline")
    with p.destination_client() as client:
        # Example queries - uncomment or edit as needed
        q = "SELECT * FROM nyc_taxi_data.trips LIMIT 10"
        with client.execute_query(q) as result:
            print(result.fetchdf())

        # Homework-style queries (uncomment to run):
        # q = '''SELECT MIN("Trip_Pickup_DateTime") AS start_date, MAX("Trip_Pickup_DateTime") AS end_date FROM nyc_taxi_data.trips'''
        # q = '''SELECT COUNT(*) FILTER (WHERE "Payment_Type" = 'Credit') * 100.0 / COUNT(*) AS pct_credit FROM nyc_taxi_data.trips'''
        # q = '''SELECT SUM("Tip_Amt") AS total_tips FROM nyc_taxi_data.trips'''

if __name__ == "__main__":
    main()
