SELECT 
	regions.short_name,
	count(forecasts.id)
FROM forecasts 
	INNER JOIN models ON models.id = forecasts.model_id
	INNER JOIN regions ON regions.id = forecasts.region_id
WHERE
	models.short_name = 'HWRF'
GROUP BY
	regions.short_name
ORDER BY 
	count(forecasts.id) DESC;