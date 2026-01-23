# Homework 1

## Question 1: pip Version in python:3.13 Image (1 point)

**What's the version of pip in the python:3.13 image?**

- [ ] 25.3
- [ ] 24.3.1
- [ ] 24.2.1
- [ ] 23.3.1

**Answer Query:**
```bash
docker run --rm python:3.13 pip --version
```

---

## Question 2: Docker Compose - PgAdmin Database Connection (1 point)

**Given the docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database?**

- [ ] postgres:5433
- [ ] localhost:5432
- [ ] db:5433
- [ ] postgres:5432
- [ ] db:5432

---

## Question 3: November 2025 - Trips with Distance ≤ 1 Mile (1 point)

**For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile?**

- [ ] 7,853
- [ ] 8,007
- [ ] 8,254
- [ ] 8,421

**Answer Query:**
```sql
SELECT COUNT(*) FROM green_taxi_df
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01' 
  AND trip_distance <= 1
```

---

## Question 4: Pick Up Day with Longest Trip Distance (1 point)

**Which was the pick up day with the longest trip distance? (Only trips with trip_distance < 100 miles)**

- [ ] 2025-11-14
- [ ] 2025-11-20
- [ ] 2025-11-23
- [ ] 2025-11-25

**Answer Query:**
```sql
SELECT lpep_pickup_datetime, max(trip_distance) 
FROM public.green_taxi_df 
WHERE trip_distance < 100
GROUP BY lpep_pickup_datetime
ORDER BY 2 DESC;
```

---

## Question 5: Pickup Zone with Largest Total Amount on Nov 18th (1 point)

**Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?**

- [ ] East Harlem North
- [ ] East Harlem South
- [ ] Morningside Heights
- [ ] Forest Hills

**Answer Query:**
```sql
SELECT "Zone", SUM(total_amount) 
FROM public.green_taxi_df 
JOIN public.taxi_zones ON "PULocationID" = "LocationID"
GROUP BY "Zone"
ORDER BY 2 DESC;
```

---

## Question 6: Drop Off Zone with Largest Tip from East Harlem North (1 point)

**For passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?**

- [ ] JFK Airport
- [ ] Yorkville West
- [ ] East Harlem North
- [ ] LaGuardia Airport

**Answer Query:**
```sql
SELECT "Zone", SUM(total_amount) 
FROM public.green_taxi_df 
JOIN public.taxi_zones ON "PULocationID" = "LocationID"
GROUP BY "Zone"
ORDER BY 2 DESC;
```