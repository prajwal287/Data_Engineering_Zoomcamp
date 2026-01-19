#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--user', default='root', help='PostgreSQL username')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default='5432', help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='yellow_taxi_data', help='Target table name')
@click.option('--year', default=2021, type=int, help='Year of data to ingest')
@click.option('--month', default=1, type=int, help='Month of data to ingest')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
def run(user, password, host, port, db, table, year, month, chunksize):

    # Read a sample of the data
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    df = pd.read_csv(f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz', nrows=100)


    df = pd.read_csv(
        f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates
    )

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
 

    df_iter = pd.read_csv(f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz',
                      dtype=dtype,
                      parse_dates = parse_dates, 
                      iterator=True,
                      chunksize = chunksize)
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df.head(0).to_sql(name = table, con=engine, if_exists = 'replace')
            first = False
        df_chunk.to_sql(name=table, con=engine, if_exists='append')


if __name__ == '__main__':
    run()