import argparse
import requests
import re
import os
from datetime import datetime, timedelta 
from pathlib import Path
from loguru import logger
from tempfile import TemporaryDirectory

from tcdb.pipeline import utils, fs_utils
from tcdb.etl import atcf, atcf_forecasts
from tcdb.models import database
from tcdb.config import settings

from IPython.core.debugger import set_trace

NOW = datetime.now()


def getFileNames(response, pattern):
    pattern = re.compile(pattern)
    file_list = pattern.findall(response.text)
    response.close()
    return set(file_list) 


def downloadLocally(url, local_path):
    logger.debug(f"Downloading: {url}")
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

def processAdeck(
    input_path,
    output_dir,
    storm,
    models=settings.atcf.adeck.models,
    staging_dir=None,
    date_time=None,
    backfill=False
):
    """Process the provided ADECK file and save the output in a file that contains a single model and datetime per file. If an output file already
    exists it will only be saved if the forecast initialization datetime is less than 48 hours old.

    If you want to process a single initialization datetime and it is older than 7 days you need to set `backfill` to True

    Args:
        input_path (pathlib.Path): Path to the ADECK file
        output_dir (pathlib.Path): Directory where the processed output will be saved 
        storm (tcdb.models.Storm): Storm object representing a single record in the storms table
        models (list, optional): List of NHC defined model short-names to be parsed from the ADECK file. Defaults to the definition in `settings.yml'.
        staging_dir (pathlib.Path, optional): Path to temporarily store files to be added to the DB
        date_time (_type_, optional): Parse only the models that were initialized on `date_time`. If None, all initialization datetimes are processed. Defaults to None.
        backfill (bool, optional): If True, process the file regardless of `storm.status`. If False, only process the file if the  `storm.status` == "Active". Defaults to False.

    Returns:
        (list[pathlib.Path]): List of paths to files in the staging directory 
    """
    # parse the file into a pandas df
    df = atcf.parse_aDeck(input_path)

    if backfill is False:
        hours_from_init = 48
        # dont wast time processing files for storms that are archived
        if storm.status == "Archive":
            logger.debug(f"Storm {storm.id} [{storm.name}] status is set to 'Archive'. To force processing set `backfill` to True")
            return list()
    else:
        hours_from_init = 100000 # if we're backfilling we want to process everything
        
            
    logger.trace(f"Processing {input_path.name}")
    logger.info(f"Saving adeck output to: {output_dir.as_posix()}")
    region = database.getRegionShort(storm.region_id)

    # make sure a staging directory is defined
    if staging_dir is None:
        staging_dir = Path(settings.paths.staging_dir).joinpath(f'adeck')
        staging_dir.mkdir(exist_ok=True, parents=True)

    output_files = list()
    if date_time is None: 
        df = df.loc[df.TECH.isin(models)]
        for DATETIME, dat in df.groupby('DATETIME'):
            for TECH, d in dat.groupby('TECH'):
                output_file = output_dir.joinpath(f"{region.lower()}-{storm.id}-{DATETIME.year}_{TECH}_{DATETIME.strftime('%Y%m%d%H')}.csv")
                if output_file.exists():
                    # only save the file if the forecast datetime is less than 48 hours old (will hopefully save processing time)
                    if (NOW - DATETIME)  > timedelta(hours=hours_from_init):
                        logger.debug(f"Forecast datetime ({DATETIME.isoformat()}) is older than 24 hours. skipping.......")
                        continue
                logger.trace(f"Saving output to: {output_file.as_posix()}")
                d.to_csv(output_file, index=False)
                output_files.append(output_file)
    else:
        # only process forecasts that are no more than 24 hours older than `date_time`
        df = df.loc[(date_time - df.DATETIME) <= timedelta(hours=24)]
        for DATETIME, dat in df.groupby('DATETIME'):
            dat = dat.loc[dat.TECH.isin(models)]
            for TECH, d in dat.groupby('TECH'):
                output_file = output_dir.joinpath(f"{region.lower()}-{storm.id}-{DATETIME.year}_{TECH}_{DATETIME.strftime('%Y%m%d%H')}.csv")
                logger.trace(f"Saving output to: {output_file.as_posix()}")
                d.to_csv(output_file, index=False)
                output_files.append(output_file)
    
    # Copy the files to the staging directory to be processed
    logger.info(f"Adding {len(output_files)} track files for {storm.name} to staging directory")
    staging_list = list()
    for output_file in output_files:
        staging_path = staging_dir.joinpath(output_file.name)
        staging_path.write_text(output_file.read_text())
        staging_list.append(staging_path)

    return staging_list


def run(basin_config, season, date_time, backfill):
    # set up paths
    download_path = Path(settings.paths.temporary_dir)
    data_lake = Path(settings.paths.data_lake)
    # since theres a possiblity we will be adding 1000s of files to the staging directory we want to
    # make sure the files are being removed even when there's an exception. Using a temporary directory
    # should be a way to make sure this doesn't happen
    base_staging_dir = Path(settings.paths.staging_dir).joinpath('atcf')
    base_staging_dir.mkdir(exist_ok=True)
    with TemporaryDirectory(dir=base_staging_dir) as staging_dir_str:
        staging_dir = Path(staging_dir_str)
        logger.info(f"Using {staging_dir.as_posix()} as the staging directory")
        with TemporaryDirectory(dir=settings.paths.temporary_dir) as tmp_dir:
            download_path = Path(tmp_dir)

            for basin, basin_dict in basin_config.items():

                adeck_dir = data_lake.joinpath(f"atcf/{basin}/adeck/{season}")
                adeck_dir.mkdir(parents=True, exist_ok=True)

                url = basin_dict.get('url')
                file_pattern = basin_dict.get('pattern')

                # get list of files on the server
                response = requests.get(url)
                # parse file names from the HTML
                file_names = getFileNames(response, file_pattern)

                # download most recent version of adeck files
                logger.info(f"Downloading {len(file_names)} raw adeck files for {basin}")
                for file_name in file_names:
                    file_url = url + file_name
                    file_path = download_path.joinpath(file_name)
                    # make sure the storm already exists in the db. If not theres no point in downloading the ADECK file
                    storm = database.inferStormFromAdeck(file_path)
                    if storm is None:
                        continue
                    if downloadLocally(file_url, file_path):
                        if file_name.endswith('.gz'):
                            file_path = fs_utils.extractGZip(file_path, file_path.parent, remove=True)
                        
                        adeck_storm_path = adeck_dir.joinpath(f"{storm.annual_id:02d}")
                        adeck_storm_path.mkdir(parents=True, exist_ok=True)
                        
                        processed_files = processAdeck(file_path, adeck_storm_path, storm, staging_dir=staging_dir, date_time=date_time, backfill=backfill)
                        if len(processed_files) > 0:
                            logger.info(f"Processing {len(processed_files)} track files for {storm.name}")
                            atcf_forecasts.process_adecks(sorted(processed_files))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and process adeck files"
    )
    parser.add_argument(
        "-r",
        "--regions",
        type=str,
        default=None,
        help="NHC region to process. If option is omitted all regins will be used"
    )
    parser.add_argument(
        "-d",
        "--date_time",
        type=str,
        default=None,
        help="Datetime use to determine if an observation is outdated or not ['yyyymmddHH']",
    )
    parser.add_argument(
        "-n",
        "--now",
        action='store_true',
        help='Use the current date_time for processing the file. Overrides the datetime provided with `-d`'
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
        date_time = None
        season = NOW.year
    else:
        date_time = datetime.strptime(args.date_time, "%Y%m%d%H")
        season = date_time.year
        season = NOW.year
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
        regions = ["al", "ep", "wp", "cp", "io"]
    else:
        regions = regions.split(',')
    logger.info(f"Runing for the following regions: {regions}")
    basin_config = dict()
    for region in regions:
        if region in ['al', 'ep', 'cp']:
            basin_config[region] = {
                'pattern': settings.atcf.adeck.file_pattern.format_map({'basin': region, 'year': season}) + ".gz",
                'url': settings.atcf.adeck.nhc_url
            }
        elif region in ['wp', 'io', 'sh']:
            basin_config[region] = {
                'pattern': settings.atcf.adeck.file_pattern.format_map({'basin': region, 'year': season}),
                'url': settings.atcf.adeck.jtwc_url
            }

    backfill = args.backfill

    if args.now:
        date_time = NOW

    # get only the regions that were passed in the options
    run(basin_config=basin_config, season=season, date_time=date_time, backfill=backfill)

    processing_time = datetime.now() - NOW
    logger.info(f"Total time to run: {processing_time.total_seconds() / 60:0.1f} minutes")
    logger.info(f"Finished running {__file__}")