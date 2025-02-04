-- During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

-- Up to 1 mile
-- In between 1 (exclusive) and 3 miles (inclusive),
-- In between 3 (exclusive) and 7 miles (inclusive),
-- In between 7 (exclusive) and 10 miles (inclusive),
-- Over 10 miles

SELECT 
    SUM(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) as up_to_1_mile,
    SUM(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 ELSE 0 END) as one_to_3_miles,
    SUM(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 ELSE 0 END) as three_to_7_miles,
    SUM(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 ELSE 0 END) as seven_to_10_miles,
    SUM(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) as over_10_miles
FROM green_tripdata
WHERE lpep_pickup_datetime::date >= '2019-10-01' 
    AND lpep_dropoff_datetime::date < '2019-11-01'


-- Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.
SELECT 
    lpep_pickup_datetime::DATE AS pickup_date,
    max(trip_distance) AS max_distance
FROM green_tripdata
GROUP BY 
    lpep_pickup_datetime::DATE
ORDER BY 
    max_distance DESC;


-- Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?
WITH high_earning_zones AS (
   SELECT 
       "PULocationID"
   FROM green_tripdata
   WHERE 
       lpep_pickup_datetime::DATE = '2019-10-18'
   GROUP BY 
       "PULocationID"
   HAVING 
       SUM(total_amount) > 13000
)
SELECT 
   zl."Zone" AS zone_name
FROM taxi_zone_lookup zl
JOIN high_earning_zones hz 
   ON zl."LocationID" = hz."PULocationID"
ORDER BY 
   zone_name;


-- For the passengers picked up in Ocrober 2019 in the zone name "East Harlem North" which was the drop off zone that had the largest tip?
WITH october_trip_tips AS (
    SELECT 
        g."DOLocationID",
        dropoff."Zone" as dropoff_zone,
        SUM(g.tip_amount) as total_tips,
        RANK() OVER (ORDER BY SUM(g.tip_amount) DESC) as tip_rank
    FROM green_tripdata g
    JOIN taxi_zone_lookup pickup 
        ON g."PULocationID" = pickup."LocationID"
    JOIN taxi_zone_lookup dropoff
        ON g."DOLocationID" = dropoff."LocationID"
    WHERE pickup."Zone" = 'East Harlem North'
        AND lpep_pickup_datetime::date >= '2019-10-01' 
        AND lpep_pickup_datetime::date < '2019-11-01'
    GROUP BY 
        g."DOLocationID",
        dropoff."Zone"
)
SELECT
	"DOLocationID",
    dropoff_zone, 
	total_tips
FROM october_trip_tips
WHERE tip_rank = 1;