SELECT 
	tracks.id,
	tracks.ensemble_number,
	storms.name,
	storms.annual_id,
	storms.nhc_id,
	steps.hour,
	steps.latitude,
	steps.longitude,
	steps.intensity_kts,
	steps.mslp_mb,
	forecasts.id,
	models.short_name,
	forecasts.region_id,
	forecasts.datetime_utc
FROM tracks
	INNER JOIN steps ON steps.track_id = tracks.id
	INNER JOIN storms ON storms.id = tracks.storm_id
	INNER JOIN forecasts ON forecasts.id = tracks.forecast_id
	INNER JOIN models ON models.id = forecasts.model_id
WHERE
	tracks.storm_id = 98
	AND
	forecasts.datetime_utc = '2022-09-28 00:00:00';