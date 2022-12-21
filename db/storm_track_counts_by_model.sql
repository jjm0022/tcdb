SELECT 
	storms.name,
	models.short_name,
	count(tracks.id)
FROM tracks
	INNER JOIN forecasts ON forecasts.id = tracks.forecast_id
	INNER JOIN models ON models.id = forecasts.model_id
	INNER JOIN regions ON regions.id = forecasts.region_id
	INNER JOIN storms on storms.id = tracks.storm_id
WHERE
	regions.short_name = 'WP'
GROUP BY
	models.short_name, storms.id
ORDER BY 
	storms.nhc_number ASC, count(tracks.id) DESC;