-- Aggregate model: watering history per plant, compared with its water needs (seeds.plant_water_needs).
-- Grain: one row per plant.

SELECT
	p.plant_id,
	p.plant_name,
	p.plant_type,
	p.location,
	COUNT(*)::BIGINT                                                            AS days_recorded,
	ROUND(SUM(p.water_ml), 1)                                                   AS total_water_ml,
	ROUND(AVG(p.water_ml), 1)                                                   AS avg_water_ml,
	ROUND(MIN(p.water_ml), 1)                                                   AS min_water_ml,
	ROUND(MAX(p.water_ml), 1)                                                   AS max_water_ml,
	ROUND(AVG(p.soil_moisture_pct), 1)                                          AS avg_soil_moisture_pct,
	MAX(p.watered_at)                                                           AS last_watered_at,
	ANY_VALUE(n.daily_min_ml)                                                   AS need_min_ml,
	ANY_VALUE(n.daily_max_ml)                                                   AS need_max_ml,
	ROUND(100 * AVG(p.water_ml) / ANY_VALUE((n.daily_min_ml + n.daily_max_ml) / 2.0), 1) AS avg_pct_of_target
FROM current.plant_watering_daily AS p
LEFT JOIN seeds.plant_water_needs AS n USING (plant_id)
GROUP BY p.plant_id, p.plant_name, p.plant_type, p.location
ORDER BY p.plant_id
