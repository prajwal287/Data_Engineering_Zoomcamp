-- Question 1. What is count of records for the 2024 Yellow Taxi Data? 
-- 1. Create a single external table that looks at ALL 2024 files
CREATE OR REPLACE EXTERNAL TABLE `psyched-loader-485321-a8.zoomcamp.yellow_2024_combined`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://prajwal-nandeeshappa-028/yellow_tripdata_2024-*.parquet']
);
-- 2. Get the total count
SELECT COUNT(*) AS total_records
FROM `psyched-loader-485321-a8.zoomcamp.yellow_2024_combined`;
-- Answer: 20,332,093


-- Question 2. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table? 
-- For the External Table:
SELECT DISTINCT(PULocationID) FROM `psyched-loader-485321-a8.zoomcamp.yellow_2024_ext`;
-- For the Materialized Table:
SELECT DISTINCT(PULocationID) FROM `psyched-loader-485321-a8.zoomcamp.yellow_2024_combined`;
-- Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table


-- Question 3. Why are the estimated number of Bytes different? 
-- Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

-- Question 4. How many records have a fare_amount of 0?
SELECT count(*) FROM `psyched-loader-485321-a8.zoomcamp.yellow_2024_combined`
where fare_amount = 0;
-- Answer: 8,333


-- Question 5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy) 
CREATE OR REPLACE TABLE `psyched-loader-485321-a8.zoomcamp.yellow_tripdata_2024_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `psyched-loader-485321-a8.zoomcamp.yellow_2024_combined`;
-- Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID



-- Question 6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?
-- Query 1 (Non-Partitioned):
SELECT DISTINCT VendorID
FROM `psyched-loader-485321-a8.zoomcamp.yellow_2024_combined`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Estimator says: 310.24 MB
-- Query 2 (Partitioned):
SELECT DISTINCT VendorID
FROM `psyched-loader-485321-a8.zoomcamp.yellow_tripdata_2024_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
-- Estimator says: 26.84 MB
-- Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table


