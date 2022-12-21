SELECT 
	tracks.track_id,
	tracks.ensemble_number,
	storms.name,
	storms.id,
	storms.nhc_id,
	steps.hour,
	steps.latitude,
	steps.longitude,
	steps.intensity_kts,
	steps.mslp_mb,
	forecasts.forecast_id,
	models.short_name,
	forecasts.region_id,
	forecasts.datetime_utc
FROM tracks
	INNER JOIN steps ON steps.track_id = tracks.track_id
	INNER JOIN storms ON storms.storm_id = tracks.storm_id
	INNER JOIN forecasts ON forecasts.forecast_id = tracks.forecast_id
	INNER JOIN models ON models.model_id = forecasts.model_id
	
WHERE
	tracks.storm_id = 37
	AND
	forecasts.model_id = 30;