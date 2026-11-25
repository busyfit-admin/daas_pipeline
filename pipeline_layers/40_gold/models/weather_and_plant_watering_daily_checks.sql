-- Fact model: each plant's watering on each day, checked against its water needs and that day's weather.
-- Grain: one row per plant per date (from current.plant_watering_daily).
--
-- watering_check: ok | under_watered | over_watered | watered_on_rain_day | no_water_needs
-- soil_check:     ok | dry | wet | no_water_needs

WITH joined AS (
	SELECT
		p.date,
		p.plant_id,
		p.plant_name,
		p.plant_type,
		p.location,
		p.watered_at,
		p.water_ml,
		p.soil_moisture_pct,
		w.avg_temp_max_c,
		w.avg_rain_mm,
		w.date IS NOT NULL                                        AS has_weather,
		n.daily_min_ml,
		n.daily_max_ml,
		n.min_soil_moisture_pct,
		n.max_soil_moisture_pct,
		COALESCE(w.avg_temp_max_c >= n.hot_day_temp_c, FALSE)     AS is_hot_day,
		COALESCE(w.avg_rain_mm >= n.rain_skip_mm, FALSE)          AS is_rain_day,
		n.hot_day_extra_pct
	FROM current.plant_watering_daily AS p
	LEFT JOIN gold.weather_daily AS w USING (date)
	LEFT JOIN seeds.plant_water_needs AS n USING (plant_id)
),

targets AS (
	SELECT
		*,
		ROUND(daily_min_ml * CASE WHEN is_hot_day THEN 1 + hot_day_extra_pct / 100.0 ELSE 1 END, 1) AS target_min_ml,
		ROUND(daily_max_ml * CASE WHEN is_hot_day THEN 1 + hot_day_extra_pct / 100.0 ELSE 1 END, 1) AS target_max_ml
	FROM joined
)

SELECT
	date,
	plant_id,
	plant_name,
	plant_type,
	location,
	watered_at,
	water_ml,
	target_min_ml,
	target_max_ml,
	ROUND(CASE
		WHEN water_ml < target_min_ml THEN water_ml - target_min_ml
		WHEN water_ml > target_max_ml THEN water_ml - target_max_ml
		ELSE 0
	END, 1)                                                       AS water_gap_ml,
	soil_moisture_pct,
	min_soil_moisture_pct,
	max_soil_moisture_pct,
	has_weather,
	avg_temp_max_c,
	avg_rain_mm,
	is_hot_day,
	is_rain_day,
	CASE
		WHEN daily_min_ml IS NULL                 THEN 'no_water_needs'
		WHEN is_rain_day AND water_ml > 0         THEN 'watered_on_rain_day'
		WHEN is_rain_day                          THEN 'ok'
		WHEN water_ml < target_min_ml             THEN 'under_watered'
		WHEN water_ml > target_max_ml             THEN 'over_watered'
		ELSE 'ok'
	END                                                           AS watering_check,
	CASE
		WHEN min_soil_moisture_pct IS NULL               THEN 'no_water_needs'
		WHEN soil_moisture_pct < min_soil_moisture_pct   THEN 'dry'
		WHEN soil_moisture_pct > max_soil_moisture_pct   THEN 'wet'
		ELSE 'ok'
	END                                                           AS soil_check,
	watering_check = 'ok' AND soil_check = 'ok'                   AS all_checks_ok
FROM targets
ORDER BY date, plant_id
