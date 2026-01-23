#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


@click.command()
@click.option('--user', default='root', help='PostgreSQL username')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default='5432', help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--yellow-table', default='yellow_taxi_df', help='Target table name for yellow taxi')
@click.option('--green-table', default='green_taxi_df', help='Target table name for green taxi')
@click.option('--year', default=2025, type=int, help='Year of data to ingest')
@click.option('--month', default=11, type=int, help='Month of data to ingest')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading parquet')
def run(user, password, host, port, db, yellow_table, green_table, year, month, chunksize):
    """
    Ingest yellow and green taxi data for a given year and month.
    Data is read from parquet files and loaded into PostgreSQL database.
    """
    
    # Create database engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # Ingest Yellow Taxi Data
    print(f"Ingesting yellow taxi data for {year}-{month:02d}...")
    yellow_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
    yellow_taxi_df = pd.read_parquet(yellow_url)
    print(f"Yellow taxi rows: {len(yellow_taxi_df)}")
    
    # Process yellow taxi data in chunks
    total_rows = len(yellow_taxi_df)
    num_chunks = (total_rows // chunksize) + 1
    first = True
    
    for i in tqdm(range(num_chunks), desc="Inserting yellow taxi data"):
        start_idx = i * chunksize
        end_idx = min((i + 1) * chunksize, total_rows)
        df_chunk = yellow_taxi_df.iloc[start_idx:end_idx]
        
        if len(df_chunk) == 0:
            continue
        
        if first:
            df_chunk.head(0).to_sql(name=yellow_table, con=engine, if_exists="replace")
            first = False
        
        df_chunk.to_sql(name=yellow_table, con=engine, if_exists="append")
    
    print(f"Yellow taxi data ingestion complete!\n")
    
    # Ingest Green Taxi Data
    print(f"Ingesting green taxi data for {year}-{month:02d}...")
    green_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'
    green_taxi_df = pd.read_parquet(green_url)
    print(f"Green taxi rows: {len(green_taxi_df)}")
    
    # Process green taxi data in chunks
    total_rows = len(green_taxi_df)
    num_chunks = (total_rows // chunksize) + 1
    first = True
    
    for i in tqdm(range(num_chunks), desc="Inserting green taxi data"):
        start_idx = i * chunksize
        end_idx = min((i + 1) * chunksize, total_rows)
        df_chunk = green_taxi_df.iloc[start_idx:end_idx]
        
        if len(df_chunk) == 0:
            continue
        
        if first:
            df_chunk.head(0).to_sql(name=green_table, con=engine, if_exists="replace")
            first = False
        
        df_chunk.to_sql(name=green_table, con=engine, if_exists="append")
    
    print(f"Green taxi data ingestion complete!\n")
    
    # Ingest Taxi Zones Lookup Data
    print("Ingesting taxi zones lookup data...")
    zones_df = pd.read_csv('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv')
    print(f"Taxi zones rows: {len(zones_df)}")
    print(pd.io.sql.get_schema(zones_df, name='taxi_zones', con=engine))
    zones_df.head(0).to_sql(name='taxi_zones', con=engine, if_exists='replace')
    zones_df.to_sql(name='taxi_zones', con=engine, if_exists='append')
    print(f"Taxi zones data ingestion complete!")


if __name__ == '__main__':
    run()
