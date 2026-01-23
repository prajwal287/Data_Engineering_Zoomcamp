Homework 1

Question 1. What's the version of pip in the python:3.13 image? (1 point)
25.3
24.3.1
24.2.1
23.3.1

Answer query: docker run --rm python:3.13 pip --version

Question 2. Given the docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database? (1 point)
postgres:5433
localhost:5432
db:5433
postgres:5432
db:5432

Question 3. For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile? (1 point)
7,853
8,007
8,254
8,421

Answer query: 
SELECT COUNT(*) FROM green_taxi_df
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01' 
  AND trip_distance <= 1

Question 4. Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles. (1 point)
2025-11-14
2025-11-20
2025-11-23
2025-11-25  

Answer query: 
SELECT lpep_pickup_datetime, max(trip_distance) FROM public.green_taxi_df where trip_distance < 100
group by lpep_pickup_datetime
order by 2 desc;

Question 5. Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025? (1 point)
East Harlem North
East Harlem South
Morningside Heights
Forest Hills

Answer query: 
SELECT "Zone", SUM(total_amount) FROM public.green_taxi_df 
JOIN public.taxi_zones
ON "PULocationID" = "LocationID"
GROUP BY "Zone"
ORDER BY 2 DESC;

Question 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip? (1 point)
JFK Airport
Yorkville West
East Harlem North
LaGuardia Airport

Answer query: 
SELECT "Zone", SUM(total_amount) FROM public.green_taxi_df 
JOIN public.taxi_zones
ON "PULocationID" = "LocationID"
GROUP BY "Zone"
ORDER BY 2 DESC;