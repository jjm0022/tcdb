import pandas as pd
from datetime import datetime, timedelta

# https://www.emc.ncep.noaa.gov/HWRF/tcvitals-draft.html
columns = ['org', 'system_id', 'storm_name', 'date', 'time', 'latitude', 'longitude', 'storm_direction_degrees', 'storm_speed_dms', 'mslp_mb', 'env_press_mb', 'roci_km', 'vmax_ms', 'rmw_km',
'r34_ne_km',
'r34_se_km',
'r34_sw_km',
'r34_nw_km',
'storm_depth', 
'r50_ne_km',
'r50_se_km',
'r50_sw_km',
'r50_nw_km',
'max_forecast_hour',
'max_forecast_lat',
'max_forecast_lon',
'r64_ne_km',
'r64_se_km',
'r64_sw_km',
'r64_nw_km',
'storm_type',
'storm_priority'
]
df = pd.read_csv('http://hurricanes.ral.ucar.edu/repository/data/tcvitals_open/2022/combined_tcvitals.2022.dat', sep='\s+', na_values=[-9, -99, -999, '-999W', '-999N', '-99N',], names=columns)
df['date_time'] = df.apply(lambda row: datetime.strptime(str(row.date), '%Y%m%d') + timedelta(hours=int(row.time)/100), axis=1)
df = df.drop(labels=['date', 'time'], axis=1)
df["latitude"] = df.latitude.apply(lambda x: float(x[:-1]) / 10 if x.endswith("N") else -float(x[:-1]) / 10)
df["longitude"] = df.longitude.apply(lambda x: -float(x[:-1]) / 10 if x.endswith("W") else float(x[:-1]) / 10)

