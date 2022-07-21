from tkinter import W
import requests
import re
import os
import json
import gzip
import shutil
import pendulum
from pathlib import Path
from loguru import logger

from tcdb.pipeline import utils
from tcdb.pipeline import fs_utils
from tcdb.etl import atcf

from IPython.core.debugger import set_trace

now = pendulum.now("UTC")
timestamp = now.strftime("%Y%m%dT%H%M")
ADECK_PATTERN = search_pattern = f'a[aec][lp][012349][0123456789]{now.year}.dat.gz'
ADECK_URL = 'https://ftp.nhc.noaa.gov/atcf/aid_public/'
backfill = True


def getFileNames(response, pattern):
    pattern = re.compile(pattern)
    file_list = pattern.findall(response.text)
    response.close()
    return set(file_list) 


def downloadLocally(url, local_path):
    try:
        response = requests.get(url, stream=True)
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
    except Exception as e:
        logger.error(e)
        return False
    return True

def processAdeck(path, models=['HWRF', 'AVNO', 'AEMN', 'HMON', 'HCCA', 'IVCN']):
    df = atcf.parse_aDeck(path)
    storm_number = df.SNUM.unique()[0]
    basin = df.BASIN.unique()[0]
    output_dir = path.parent.joinpath(f"{storm_number:02d}")
    output_dir.mkdir(exist_ok=True)
    logger.info(f"Saving adeck output to: {output_dir.as_posix()}")
    
    df = df.loc[df.TECH.isin(models)]
    for DATETIME, dat in df.groupby('DATETIME'):
        for TECH, d in dat.groupby('TECH'):
            output_file = output_dir.joinpath(f"{basin.lower()}{storm_number:02d}{DATETIME.year}_{TECH}_{DATETIME.strftime('%Y%m%d%H')}.csv")
            d.to_csv(output_file, index=False)


if __name__ == "__main__":
    basin_config = {
        'al': {
            'pattern': f'aal[012349][0123456789]{now.year}.dat.gz',
            'url': 'https://ftp.nhc.noaa.gov/atcf/aid_public/'
        },
        'ep': {
            'pattern': f'aep[012349][0123456789]{now.year}.dat.gz',
            'url': 'https://ftp.nhc.noaa.gov/atcf/aid_public/'
        },
        'cp': {
            'pattern': f'acp[012349][0123456789]{now.year}.dat.gz',
            'url': 'https://ftp.nhc.noaa.gov/atcf/aid_public/'
        },
        'wp': {
            'pattern': f'awp[012349][0123456789]{now.year}.dat',
            'url': 'https://www.ssd.noaa.gov/PS/TROP/DATA/ATCF/JTWC/',
        },
        'io': {
            'pattern': f'aio[012349][0123456789]{now.year}.dat',
            'url': 'https://www.ssd.noaa.gov/PS/TROP/DATA/ATCF/JTWC/',
        },
        'sh': {
            'pattern': f'ash[012349][0123456789]{now.year}.dat',
            'url': 'https://www.ssd.noaa.gov/PS/TROP/DATA/ATCF/JTWC/',
        },
    }

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

    for basin, basin_dict in basin_config.items():

        adeck_dir = data_lake.joinpath(f"atcf/{basin}/adeck/{now.year}")
        adeck_dir.mkdir(parents=True, exist_ok=True)
        storm_dir = data_lake.joinpath(f"atcf/{basin}/storm/{now.year}")
        storm_dir.mkdir(parents=True, exist_ok=True)
        url = basin_dict.get('url')
        file_pattern = basin_dict.get('pattern')

        url = basin_dict.get('url')
        file_pattern = basin_dict.get('pattern')
  
        # get list of files on the server
        response = requests.get(url)
        # parse file names from the HTML
        file_names = getFileNames(response, file_pattern)

        # download most recent version of adeck files
        logger.info(f"Downloading {len(file_names)} files for {basin}")
        for file_name in file_names:
            file_url = url + file_name
            file_path = adeck_dir.joinpath(file_name)
            downloadLocally(file_url, file_path)
            if file_name.endswith('.gz'):
                file_path = fs_utils.extractGZip(file_path, file_path.parent, remove=True)
            processAdeck(file_path)



    # remove any of the files that were just downloade that have the same contents as an existing file
    #fs_utils.removeDuplicateFiles(adeck_dir, "*.csv")

    # parse the adeck files and save the data to json files
    #processadecks(adeck_dir, storm_dir, backfill=backfill)

    # remove and of the new storm files that have the same contents as existing files
    #fs_utils.removeDuplicateFiles(storm_dir, "*.json")
                    
    logger.info(f"Finished running {__file__}")
