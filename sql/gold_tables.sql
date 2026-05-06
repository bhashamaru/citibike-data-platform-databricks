-- find the end station most crowded with bikes
CREATE OR REPLACE TABLE gold.station_popularity AS
SELECT end_station_name, count(*) AS station_count
FROM silver.citibike_trips
WHERE end_station_id IS NOT NULL --removing dockless bikes for this visualization
AND is_suspicious_duration = false --removing dirty rows where ended_at is less than started_at
GROUP BY end_station_name