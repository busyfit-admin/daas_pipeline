-- Top 20 rows from each current-layer table (one view per table, see tools/query_current.py)
-- Run with: make query FILE=queries/top20_current.sql

-- Weather: most recent days first
SELECT *
FROM weather_daily
ORDER BY date DESC, latitude, longitude
LIMIT 20;

-- Plant watering: most recently loaded first
SELECT *
FROM plant_watering_daily
ORDER BY last_updated DESC, plant_id
LIMIT 20;
