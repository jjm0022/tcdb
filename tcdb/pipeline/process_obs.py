import os
import json
import pendulum
from pathlib import Path
from loguru import logger
from collections import defaultdict

from tcdb.pipeline import utils
from tcdb.pipeline import fs_utils

from IPython.core.debugger import set_trace

now = pendulum.now("UTC")
timestamp = now.strftime("%Y%m%dT%H%M")
backfill = True

def getUniqueID(lines):
    basin = lines[0][0].upper()
    start_date = pendulum.from_format(str(lines[0][2]), "YYYYMMDDHH", tz='UTC')
    start_lat = lines[0][6]
    if start_lat.endswith("N"):
        start_lat = round(float(start_lat[:-1]) * 0.1, 1)
    elif start_lat.endswith("S"):
        start_lat = round(float(start_lat[:-1]) * -0.1, 1)
    
    #return f"{start_date.format('YYYYMMDDHH')}{basin}{int(start_lat):02d}"
    return f"{basin}{start_date.format('YYYYMMDDHH')}.{int(start_lat):02d}"


def parse_obs(path):
    assert path.exists(), f"Bdeck file does not exist: {path.as_posix()}"
    obs_dict = defaultdict(list)
    with open(path, 'r') as t:
        lines = t.readlines()
    lines = [(l.strip().replace(" ", "")).split(",") for l in lines]
    lines = [line for line in lines if len(line) >= 27]  # dont keep lines that dont at lest have storm name
    uid = getUniqueID(lines) # {start_date}{basin}{start_lat}
    for line in lines:
        ob = {}
        date_time = pendulum.from_format(line[2], "YYYYMMDDHH", tz="UTC")
        ob['date_time'] = date_time.isoformat()
        ob['basin'] = line[0].upper()
        ob['nhc_num'] = int(line[1])
        lat = line[6]
        lon = line[7]
        if lat.endswith("N"):
            lat = round(float(lat[:-1]) * 0.1, 1)
        elif lat.endswith("S"):
            lat = round(float(lat[:-1]) * -0.1, 1)
        if lon.endswith("W"):
            lon = round(float(lon[:-1]) * -0.1, 1)
        elif lon.endswith("E"):
            lon = round(float(lon[:-1]) * 0.1, 1)
        ob['lat'] = lat
        ob['lon'] = lon
        ob['wind'] = int(line[8])
        ob['mslp'] = int(line[9])
        ob['radius'] = int(line[11])
        ob['ne'] = int(line[13])
        ob['se'] = int(line[14])
        ob['sw'] = int(line[15])
        ob['nw'] = int(line[16])
        ob['pouter'] = int(line[17])
        ob['router'] = int(line[18])
        ob['gusts'] = int(line[19])
        ob['storm_name'] = line[27]

        ob['nhc_id'] = f"{ob['basin'].upper()}{ob['nhc_num']:02d}{date_time.year}"
        obs_dict[date_time.format("YYYYMMDDHH")].append(ob)

    return uid, obs_dict
    

if __name__ == "__main__":
    # configure logger
    if os.environ.get('RUN_BY_CRON', 0):
        log_name = f"{__file__.split('/')[-1].split('.')[0]}.log"
        level = "INFO"
    else:
        log_name = None
        level = "DEBUG"
    config = utils.get_logger_config(log_name, level) 
    logger.configure(**config)
    logger.info(f"Starting {__file__}")

    # set up paths
    data_lake = Path(f"/Work_Data/tcdb/data/lake")
    bdeck_dir = data_lake.joinpath(f"real_time/{now.year}/bdeck")
    bdeck_dir.mkdir(parents=True, exist_ok=True)
    obs_dir = data_lake.joinpath(f"real_time/{now.year}/observations")
    obs_dir.mkdir(parents=True, exist_ok=True)

    for bdeck_file in sorted(bdeck_dir.glob("*.csv")):
        logger.info(f'Processing {bdeck_file.name}')
        timestamp = bdeck_file.name.split('.')[0].split('_')[-1]
        uid, obs_dict = parse_obs(bdeck_file)
        storm_dir = obs_dir.joinpath(uid)
        storm_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Saving {len(obs_dict)} observations for {uid}")
        for date_time, ob in obs_dict.items():
            file_name = f"{uid}_{date_time}_{timestamp}.json"
            with open(storm_dir.joinpath(file_name), 'w') as j:
                json.dump(ob, j, indent=2, sort_keys=True)
        
        
