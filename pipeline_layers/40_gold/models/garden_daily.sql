-- Aggregate model: the whole garden per day, total water given vs. the plants' combined target.
-- Grain: one row per date.

SELECT
	p.date,
	COUNT(*)::BIGINT                                                      AS plants_watered,
	ROUND(SUM(p.water_ml), 1)                                             AS total_water_ml,
	ROUND(SUM((n.daily_min_ml + n.daily_max_ml) / 2.0), 1)                AS total_target_ml,
	ROUND(100 * SUM(p.water_ml) / SUM((n.daily_min_ml + n.daily_max_ml) / 2.0), 1) AS pct_of_target,
	ROUND(AVG(p.soil_moisture_pct), 1)                                    AS avg_soil_moisture_pct,
	MIN(p.watered_at)                                                     AS first_watered_at,
	MAX(p.watered_at)                                                     AS last_watered_at
FROM current.plant_watering_daily AS p
LEFT JOIN seeds.plant_water_needs AS n USING (plant_id)
GROUP BY p.date
ORDER BY p.date
