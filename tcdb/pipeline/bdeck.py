import argparse
import requests
import re
import os
import json
import pendulum
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger
from tempfile import TemporaryDirectory


from tcdb.pipeline import utils
from tcdb.pipeline import fs_utils
from tcdb.config import settings

from tcdb.etl.process_storms import processStorms
from tcdb.etl.process_obs import processObservations
#from tcdb.etl import Invest

from IPython.core.debugger import set_trace

NOW = pendulum.now("UTC")
timestamp = NOW.strftime("%Y%m%dT%H%M")


def getFileNames(response, pattern):
    pattern = re.compile(pattern)
    file_list = pattern.findall(response.text)
    response.close()
    return set(file_list) 


def downloadLocally(url, local_path, verify=True):

    try:
        response = requests.get(url, verify=verify)
        with open(local_path, 'w') as f:
            f.write(response.text)
    except Exception as e:
        logger.error(e)
        return False
    return True


def run(basin_config, date_time, force, backfill=False):
    """_summary_

    TODO: incorporate backfill

    Args:
        basin_config (_type_): _description_
        date_time (_type_): _description_
        force (_type_): _description_
        backfill (_type_): _description_
    """
    # set up paths
    download_path = Path(settings.paths.temporary_dir)
    staging_dir = Path(settings.paths.staging_dir).joinpath('bdeck')
    staging_dir.mkdir(exist_ok=True, parents=True)
    data_lake = Path(settings.paths.data_lake)

    with TemporaryDirectory(dir=settings.paths.temporary_dir) as tmp_dir:
        download_path = Path(tmp_dir)

        for basin, basin_dict in basin_config.items():

            bdeck_dir = data_lake.joinpath(f"atcf/{basin}/bdeck/{date_time.year}")
            bdeck_dir.mkdir(parents=True, exist_ok=True)

            url = basin_dict.get('url')
            file_pattern = basin_dict.get('pattern')

            # https://tropycal.github.io/tropycal/api/generated/tropycal.realtime.Realtime.html#tropycal.realtime.Realtime:~:text=return%20online%20again.-,Warning,-JTWC%E2%80%99s%20SSL%20certificate
            if "nrlmry" in url:
                logger.warning("Not using SSL certification")
                verify = False
            else:
                verify = True

            # get list of files on the server
            response = requests.get(url, verify=verify)
            # parse file names from the HTML
            file_names = getFileNames(response, file_pattern)

            files_to_staging = 0
            # download most recent version of bdeck files
            logger.info(f"Downloading {len(file_names)} files for {basin}")
            for file_name in file_names:
                file_url = url + file_name
                tmp_path = download_path.joinpath(file_name)
                if downloadLocally(file_url, tmp_path, verify=verify):
                    # check to see if the contents of the file have been updated
                    if fs_utils.isContentsUnique(tmp_path, bdeck_dir.glob(f"{file_name.split('.')[0]}*")):
                        # work-around for the bug where WP bdecks are randomly empty on the JTWC data site
                        if tmp_path.stat().st_size == 0:
                            logger.error(f'{tmp_path.as_posix()} is empty. Not replacing')
                            continue
                        logger.info(f"{file_name} has been updated")
                        final_path = bdeck_dir.joinpath(f"{file_name.split('.')[0]}_{timestamp}.csv")
                        staging_path = staging_dir.joinpath(f"{file_name.split('.')[0]}_{timestamp}.csv")
                        # move the file from the temporary dir to the data lake path
                        tmp_path.rename(final_path)
                        # copy the file from the data lake to the staging direcory
                        logger.info(f"Copying {final_path.as_posix()} to staging directory for processing")
                        staging_path.write_text(final_path.read_text())
                        files_to_staging += 1
                    else:
                        if force:
                            # get the file with the oldest timestamp
                            most_recent_file = list(sorted(bdeck_dir.glob(f"{file_name.split('.')[0]}*")))[-1]
                            logger.info(f"`force` is True. Copying {most_recent_file.as_posix()} to staging directory")
                            staging_path = staging_dir.joinpath(f"{most_recent_file.name}")
                            # copy to staging directory
                            staging_path.write_text(most_recent_file.read_text())
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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Download and process bdeck files"
    )
    parser.add_argument(
        "-r",
        "--regions",
        type=str,
        default=None,
        help="NHC region to process. If option is omitted all regins will be used. Multiple regions should be separated with a comma `,`"
    )
    parser.add_argument(
        "-d",
        "--date_time",
        type=str,
        default=None,
        help="Datetime use to determine if an observation is outdated or not ['yyyymmddHH']",
    )
    parser.add_argument(
        '-u',
        '--update_invests',
        action='store_true',
        help="Update static the invest file for the provided 'date_time'"
    )
    parser.add_argument(
        "-f",
        "--force",
        action='store_true',
        help='Force processing for the oldest version of all bdeck files in the season.'
    )
    parser.add_argument(
        '-b',
        '--backfill',
        action='store_true',
        help="Process files regardless of forecast initialization datetime"
    )
    parser.add_argument(
        "-l",
        "--loglevel",
        type=str,
        default="DEBUG",
        choices=["INFO", "DEBUG", "TRACE"],
        help="Level to set the logger to.",
    )

    args = parser.parse_args()
    # configure logger
    if os.environ.get('RUN_BY_CRON', 0):
        log_name = f"{__file__.split('/')[-1].split('.')[0]}.log"
        level = "INFO"
        config = utils.get_logger_config(log_name, level) 
        logger.configure(**config)
        logger.info("Running logging with CRONTAB configuration")
        logger.info(f"Log level has been set to: {level}")
    else:
        log_name = None
        level = args.loglevel
        config = utils.get_logger_config(log_name, level) 
        logger.configure(**config)
        logger.info(f"Log level has been set to: {level}")

    logger.info(f"Starting {__file__}")

    if args.date_time is None:
        date_time = NOW
    else:
        date_time = datetime.strptime(args.date_time, "%Y%m%d%H")
    # set date_time to the most recent forecast cycle hour
    # if date_time is none leave it
    if date_time is None:
        pass
    else:
        # keep subtracting an hour until we get to 0, 6, 12, or 18
        while date_time.hour not in [0, 6, 12, 18]:
            date_time = date_time - timedelta(hours=1)
        logger.info(f"Datetime has been adjusted to: {date_time.strftime('%Y%m%d%H')}")

    regions = args.regions
    if regions is None:
        # default regions
        regions = ["al", "ep", "wp", "cp", "io"]
    else:
        regions = regions.split(',')
    logger.info(f"Runing for the following regions: {regions}")
    basin_config = dict()
    for region in regions:
        if region in ['al', 'ep', 'cp']:
            basin_config[region] = {
                'pattern': settings.atcf.bdeck.file_pattern.format_map({'basin': region, 'year': date_time.year}),
                'url': settings.atcf.bdeck.nhc_url
            }
        elif region in ['wp', 'io', 'sh']:
            basin_config[region] = {
                'pattern': settings.atcf.bdeck.file_pattern.format_map({'basin': region, 'year': date_time.year}),
                'url': settings.atcf.bdeck.jtwc_url.format_map({'year': date_time.year})
            }

    force = args.force

    run(basin_config, date_time, force)

    if args.update_invests:
        # TODO
        updateInvestFile(date_time)

    logger.info(f"Finished running {__file__}")
