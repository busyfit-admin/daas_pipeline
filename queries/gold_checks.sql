-- Sample queries to check the gold layer (tables in the gold namespace are views by table name).
-- Run with: make sample-gold   (or: make query LAYER=gold FILE=queries/gold_checks.sql)

-- 1. Freshness and size: every gold table, its row count and when it was built
SELECT 'weather_daily' AS table_name, COUNT(*) AS row_count, MAX(built_at) AS built_at FROM weather_daily
UNION ALL SELECT 'plant_summary', COUNT(*), MAX(built_at) FROM plant_summary
UNION ALL SELECT 'garden_daily', COUNT(*), MAX(built_at) FROM garden_daily
UNION ALL SELECT 'weather_and_plant_watering_daily_checks', COUNT(*), MAX(built_at) FROM weather_and_plant_watering_daily_checks
UNION ALL SELECT 'seed_plant_water_needs', COUNT(*), MAX(built_at) FROM seed_plant_water_needs;

-- 2. Grain checks: each should return 0 duplicates
SELECT 'weather_daily (date)' AS model, COUNT(*) - COUNT(DISTINCT date) AS duplicates FROM weather_daily
UNION ALL SELECT 'plant_summary (plant_id)', COUNT(*) - COUNT(DISTINCT plant_id) FROM plant_summary
UNION ALL SELECT 'garden_daily (date)', COUNT(*) - COUNT(DISTINCT date) FROM garden_daily
UNION ALL SELECT 'daily_checks (plant_id, date)', COUNT(*) - COUNT(DISTINCT (plant_id, date)) FROM weather_and_plant_watering_daily_checks;

-- 3. Seed coverage: plants with watering data but no entry in the water needs seed (expect none)
SELECT plant_id, plant_name
FROM plant_summary
WHERE plant_id NOT IN (SELECT plant_id FROM seed_plant_water_needs);

-- 4. Check outcomes: how often each watering / soil result occurs
SELECT watering_check, soil_check, COUNT(*) AS plant_days
FROM weather_and_plant_watering_daily_checks
GROUP BY ALL
ORDER BY plant_days DESC;

-- 5. Latest day's problems: plants that failed a check, worst water gap first
SELECT date, plant_name, water_ml, target_min_ml, target_max_ml, water_gap_ml,
	soil_moisture_pct, watering_check, soil_check
FROM weather_and_plant_watering_daily_checks
WHERE NOT all_checks_ok
	AND date = (SELECT MAX(date) FROM weather_and_plant_watering_daily_checks)
ORDER BY ABS(water_gap_ml) DESC, plant_name;
