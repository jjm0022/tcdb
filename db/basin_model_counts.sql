SELECT 
	models.short_name,
	count(tracks.id)
FROM tracks
	INNER JOIN forecasts ON forecasts.id = tracks.forecast_id
	INNER JOIN models ON models.id = forecasts.model_id
	INNER JOIN regions ON regions.id = forecasts.region_id
WHERE
	regions.short_name = 'AL'
GROUP BY
	models.short_name
ORDER BY 
	count(tracks.id) DESC;