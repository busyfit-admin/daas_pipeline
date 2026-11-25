-- Aggregate model: regional weather per day, averaged over every grid point in current.weather_daily.
-- Grain: one row per date. Days after the load date are forecasts.

SELECT
	date,
	COUNT(*)::BIGINT                                         AS locations,
	ROUND(AVG(temperature_2m_max), 2)                        AS avg_temp_max_c,
	ROUND(MAX(temperature_2m_max), 2)                        AS max_temp_max_c,
	ROUND(AVG(temperature_2m_min), 2)                        AS avg_temp_min_c,
	ROUND(MIN(temperature_2m_min), 2)                        AS min_temp_min_c,
	ROUND(AVG(rain_sum), 2)                                  AS avg_rain_mm,
	ROUND(MAX(rain_sum), 2)                                  AS max_rain_mm,
	ROUND(AVG(epoch(sunset) - epoch(sunrise)) / 3600, 2)     AS avg_daylight_hours,
	date > CAST(MAX(last_updated) AS DATE)                   AS is_forecast
FROM current.weather_daily
GROUP BY date
ORDER BY date
