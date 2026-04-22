--create silver table with changed datatypes, cleaned data, and dervived columns

CREATE OR REPLACE TABLE workspace.silver.citibike_trips AS
select CAST(ride_id AS string) AS ride_id,
      rideable_type,
      CAST(started_at AS TIMESTAMP) AS started_at,
      CAST(ended_at AS TIMESTAMP) AS ended_at,
      start_station_id,
      start_station_name,
      end_station_id,
      end_station_name,
      CAST(start_lat AS double) AS start_lat,
      CAST(start_lng AS double) AS start_lng,
      CAST(end_lat AS double) AS end_lat,
      CAST(end_lng AS double) AS end_lng,
      member_casual,
      --add derived columns
      datediff(minute, started_at, ended_at) AS trip_duration,
      CASE WHEN trip_duration > (24 * 60) THEN true ELSE false END AS is_suspicious_duration, --marking trips longer than 24 hours as suspicious to filter out later in gold layer
      CASE WHEN end_station_id IS NULL THEN true ELSE false END AS is_dockless, --marking trips with no end station as dockless
      hour(started_at) AS hour_of_the_day,
      date_format(started_at, 'E') AS day_of_week,
      date_format(started_at, "MMM") AS month_of_trip,
      CASE WHEN date_format(started_at, 'E') in ('Sat', 'Sun') THEN true ELSE false END AS is_weekend --adding a flag for weekend to check if weekends are most popular
from bronze.citibike_trips
where ended_at > started_at; --trip duration must be positive