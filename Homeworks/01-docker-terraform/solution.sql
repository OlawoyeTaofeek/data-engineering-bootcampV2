SELECT COUNT(*)
FROM public.green_taxi_data_2025

SELECT COUNT(*) AS short_trips
FROM green_taxi_data_2025
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;

-- Question 3. For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile?
SELECT COUNT(*) AS short_trips
FROM green_taxi_data_2025
WHERE EXTRACT(MONTH FROM lpep_pickup_datetime) = 11
  AND EXTRACT(YEAR FROM lpep_pickup_datetime) = 2025
  AND trip_distance <= 1;


-- Question 4. Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles.
SELECT date(lpep_pickup_datetime) as day, MAX(trip_distance) as longest_trip_distance
from green_taxi_data_2025
WHERE trip_distance < 100
GROUP BY date(lpep_pickup_datetime)
ORDER BY 2 desc;

SELECT * FROM zones;
SELECT * FROM green_taxi_data_2025;

-- Question 5. Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?
SELECT "Zone", SUM(g.total_amount) AS total_amount_by_zone
FROM zones AS z
LEFT JOIN green_taxi_data_2025 AS g
    ON z."LocationID" = g."PULocationID"
GROUP BY "Zone"
ORDER BY total_amount_by_zone DESC
NULLS LAST;

SELECT *
FROM zones AS z
LEFT JOIN green_taxi_data_2025 AS g
    ON z."LocationID" = g."PULocationID"
WHERE "Zone" = 'East Harlem North';

-- Question 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?
SELECT z_drop."Zone" AS dropoff_zone, MAX(g.tip_amount) AS max_tip
FROM green_taxi_data_2025 AS g
JOIN zones AS z_pick
    ON g."PULocationID" = z_pick."LocationID"
JOIN zones AS z_drop
    ON g."DOLocationID" = z_drop."LocationID"
WHERE z_pick."Zone" = 'East Harlem North'
GROUP BY z_drop."Zone"
ORDER BY max_tip DESC
LIMIT 1;
