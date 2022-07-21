import requests
import re
import os
import json
import pendulum
from datetime import datetime
from pathlib import Path
from loguru import logger
from tempfile import TemporaryDirectory


from tcdb.pipeline import utils
from tcdb.pipeline import fs_utils
from tcdb.config import settings

from tcdb.etl.process_storms import processStorms
from tcdb.etl.process_obs import processObservations

from IPython.core.debugger import set_trace

now = pendulum.now("UTC")
timestamp = now.strftime("%Y%m%dT%H%M")

backfill = True


def getFileNames(response, pattern):
    pattern = re.compile(pattern)
    file_list = pattern.findall(response.text)
    response.close()
    return set(file_list) 


def downloadLocally(url, local_path):
    try:
        response = requests.get(url)
        with open(local_path, 'w') as f:
            f.write(response.text)
    except Exception as e:
        logger.error(e)
        return False
    return True


def getStormDict(text):

    lines = text.split("\n")
    lines = [(l.replace(" ", "")).split(",") for l in lines]
    lines = [line for line in lines if len(line) >= 27]  # dont keep lines that dont at lest have storm name
    start_date = pendulum.from_format(str(lines[0][2]), "YYYYMMDDHH", tz='UTC')
    end_date = pendulum.from_format(str(lines[-1][2]), "YYYYMMDDHH", tz='UTC')
    start_lat = lines[0][6]
    start_lon = lines[0][7]
    if start_lat.endswith("N"):
        start_lat = round(float(start_lat[:-1]) * 0.1, 1)
    elif start_lat.endswith("S"):
        start_lat = round(float(start_lat[:-1]) * -0.1, 1)
    if start_lon.endswith("W"):
        start_lon = round(float(start_lon[:-1]) * -0.1, 1)
    elif start_lon.endswith("E"):
        start_lon = round(float(start_lon[:-1]) * 0.1, 1)

    basin = lines[-1][0]
    winds = [int(line[8]) for line in lines]
    max_wind = 0
    for wind in winds:
        max_wind = max(wind, max_wind)

    nhc_number = int(lines[-1][1])
    nhc_id = f"{basin.upper()}{nhc_number:02d}{start_date.year}"
    storm_type = utils.getStormType(max_wind, basin)
    name = f"{storm_type}-{lines[-1][27]}"

    return dict(
        basin=basin,
        nhc_number=nhc_number,
        nhc_id=nhc_id,
        season=start_date.year,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        name=name,
        start_lat=start_lat,
        start_lon=start_lon,
    )


def processBdecks(bdeck_dir, storm_dir, backfill=False):

    files_saved = 0

    for file_path in sorted(bdeck_dir.glob(f"*.csv")):
        timestamp = file_path.name.split('.')[0].split('_')[-1]
        file_datetime = pendulum.parse(timestamp, tz='UTC')
        
        # if backfill is True, process all files
        if backfill:
            pass
        # if backfill is False, only process files that were created no more than 6 hours ago
        else:
            if (now - file_datetime).total_hours() >= 6:
                continue

        with open(file_path, 'r') as txt:
            text = txt.read()
        storm = getStormDict(text.strip())

        date_str = pendulum.parse(storm.get('start_date')).format("YYYYMMDDHH")
        file_name = f"{date_str}_{storm['nhc_id']}_{timestamp}.json"

        # only save the storm dict if it doesnt already exist
        if not storm_dir.joinpath(file_name).exists():
            logger.debug(f"Saving storm data for {storm.get('name')} to {file_name}")
            with open(storm_dir.joinpath(file_name), 'w') as j:
                # The `indent` and `sort_keys` options are needed to ensure we can consistantly 
                # check for duplicate files without these keywords the file diff doesnt work.
                json.dump(storm, j, indent=4, sort_keys=True)
            files_saved += 1
    logger.info(f"Saved {files_saved} new storm files")


if __name__ == "__main__":
    basin_config = {
        'al': {
            'pattern': f'bal[012349][0123456789]{now.year}.dat',
            'url': 'https://ftp.nhc.noaa.gov/atcf/btk/'
        },
        'ep': {
            'pattern': f'bep[012349][0123456789]{now.year}.dat',
            'url': 'https://ftp.nhc.noaa.gov/atcf/btk/'
        },
        'cp': {
            'pattern': f'bcp[012349][0123456789]{now.year}.dat',
            'url': 'https://ftp.nhc.noaa.gov/atcf/btk/'
        },
        'wp': {
            'pattern': f'bwp[012349][0123456789]{now.year}.dat',
            'url': 'https://www.ssd.noaa.gov/PS/TROP/DATA/ATCF/JTWC/',
        },
        'io': {
            'pattern': f'bio[012349][0123456789]{now.year}.dat',
            'url': 'https://www.ssd.noaa.gov/PS/TROP/DATA/ATCF/JTWC/',
        },
        'sh': {
            'pattern': f'bsh[012349][0123456789]{now.year}.dat',
            'url': 'https://www.ssd.noaa.gov/PS/TROP/DATA/ATCF/JTWC/',
        },
    }

    # configure logger
    if os.environ.get('RUN_BY_CRON', 0):
        log_name = f"{__file__.split('/')[-1].split('.')[0]}.log"
        level = "INFO"
    else:
        log_name = None
        level = "TRACE"
    config = utils.get_logger_config(log_name, level) 
    logger.configure(**config)
    logger.info(f"Starting {__file__}")

    # set up paths
    download_path = Path(settings.paths.temporary_dir)
    staging_dir = Path(settings.paths.staging_dir).joinpath('bdeck')
    staging_dir.mkdir(exist_ok=True, parents=True)
    data_lake = Path(settings.paths.data_lake)

    with TemporaryDirectory(dir=settings.paths.temporary_dir) as tmp_dir:
        download_path = Path(tmp_dir)

        for basin, basin_dict in basin_config.items():

            bdeck_dir = data_lake.joinpath(f"atcf/{basin}/bdeck/{now.year}")
            bdeck_dir.mkdir(parents=True, exist_ok=True)
            #storm_dir = data_lake.joinpath(f"atcf/{basin}/storm/{now.year}")
            #storm_dir.mkdir(parents=True, exist_ok=True)

            url = basin_dict.get('url')
            file_pattern = basin_dict.get('pattern')

            # get list of files on the server
            response = requests.get(url)
            # parse file names from the HTML
            file_names = getFileNames(response, file_pattern)

            files_to_staging = 0
            # download most recent version of bdeck files
            logger.info(f"Downloading {len(file_names)} files for {basin}")
            for file_name in file_names:
                file_url = url + file_name
                tmp_path = download_path.joinpath(file_name)
                if downloadLocally(file_url, tmp_path):
                    # check to see if the contents of the file have been updated
                    if fs_utils.isContentsUnique(tmp_path, bdeck_dir.glob(f"{file_name.split('.')[0]}*")):
                        logger.info(f"{file_name} has been updated")
                        final_path = bdeck_dir.joinpath(f"{file_name.split('.')[0]}_{timestamp}.csv")
                        staging_path = staging_dir.joinpath(f"{file_name.split('.')[0]}_{timestamp}.csv")
                        # move the file from the temporary dir to the data lake path
                        tmp_path.rename(final_path)
                        # copy the file from the data lake to the staging direcory
                        logger.info(f"Copying {final_path.as_posix()} to staging directory for processing")
                        staging_path.write_text(final_path.read_text())
                        files_to_staging += 1
                    
            logger.info(f"Added {files_to_staging} updated bdeck files to staging directory from {basin}")
            if files_to_staging > 0: 
                # process the updated bdeck files and update the storms table if necessary
                processStorms(basin.upper(), datetime.now(), staging_dir=staging_dir)
                # process the updated bdeck files and update the observations table if necessary
                processObservations(basin.upper(), date_time=None, staging_dir=staging_dir)

                # clean up
                for f in staging_dir.glob('*.csv'):
                    logger.trace(f"Removing {f.as_posix()}")
                    f.unlink()

        logger.info(f"Finished running {__file__}")
