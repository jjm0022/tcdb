SELECT 
	models.short_name,
	count(forecasts.id)
FROM forecasts 
	INNER JOIN models ON models.id = forecasts.model_id
	INNER JOIN regions ON regions.id = forecasts.region_id
WHERE
	regions.short_name = 'WP'
GROUP BY
	models.short_name
ORDER BY 
	count(forecasts.id) DESC;