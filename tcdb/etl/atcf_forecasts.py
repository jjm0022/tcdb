import argparse
import sys
import pandas as pd
from loguru import logger
from pathlib import Path
from datetime import datetime, timedelta, timezone
import warnings

warnings.filterwarnings("ignore")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tcdb.config import settings
from tcdb.etl import atcf
from tcdb.models import Forecast, Track, Step, DataSource, Model, Region, Storm
from tcdb.etl import syntracks, invest

DATE_TIME = datetime.now(tz=timezone.utc)
DATE_STR = DATE_TIME.isoformat().split(".")[0]
RUN_ID = f"FORE__{DATE_TIME.isoformat()}"


def processADECKByCycle(input_dir, output_dir, region, session, models=None, date_time=None):
    """Take each ADECK file in `input_dir` and split it up based on forecast initialization datetime. Save all forecasts for a single
    initialization file into it's own file. Adds `storm_id` to help with processing the forecasts since it is difficult to determine
    which storm a forecast belongs to without knowing the `start_date`.

    Args:
        input_dir (pathlib.Path): Path to directory of ADECK files
        output_dir (pathlib.Path): Path to directory where processed files will be saved
        region (tcdb.models.Region): Region where the files are from
        session (sqlalchemy.Session): A session that can be used to query the DB
        models (list[str], optional): List of models to process. Defaults to None.
        date_time (datetime.datetime, optional): Forecast initialization to process. Defaults to None.

    Returns:
        output_files (list[pathlib.Path]): Processed ADECK files with associated storm_id included
    """

    if models is None:
        models = ["CMC", "HMON", "HWRF", "OFCL", "LGEM", "SHIP", "AEMN", "AVNO", "EGRR"]

    cycle_dict = dict()
    for file in sorted(input_dir.glob(f"a{region.short_name.lower()}*.dat.gz")):
        # check to see if the date we want to process is in the file
        if date_time:
            if not atcf.contains_date(file, date_time):
                continue

        logger.info(f"Extracting forecasts from: {file.name}")
        # determine which storm this file belongs to. This will make processing the forecasts much easier later on
        nhc_id = file.name.split(".")[0][1:].upper()
        df = atcf.parse_aDeck(file)
        min_datetime = df.DATETIME.min()
        # minimum date in ADECK files is not guaranteed to be the same as the start_date in the BDECK files
        # this means we need to search for the storm using a range of start_dates whenever the storm is not named
        storm = (
            session.query(Storm)
            .where(Storm.nhc_id == nhc_id)
            .where(Storm.start_date >= min_datetime - timedelta(days=3))
            .where(Storm.start_date <= min_datetime - timedelta(days=1))
            .one_or_none()
        )
        if storm is None:
            logger.warning(f"Unable to determine which storm {file.name} belongs to. Moving on to next file")
            continue
        # add the storm_id to the df for later
        df["storm_id"] = storm.id

        # filter models after determining the minimum date to make sure we're getting the most accurate start_date
        df = df.loc[df.TECH.isin(models)]
        if date_time:
            df = df.loc[df.DATETIME == date_time]
        # split forecasts up based on initialization
        for cycle_datetime, steps in df.groupby("DATETIME"):
            if cycle_datetime in cycle_dict.keys():
                cycle_dict[cycle_datetime] = cycle_dict[cycle_datetime].append(steps, ignore_index=True)
            else:
                cycle_dict[cycle_datetime] = steps

    output_files = list()
    for cycle_datetime, df in cycle_dict.items():
        output_file = output_dir.joinpath(f"ATCF_{cycle_datetime.strftime('%Y%m%d%H')}_{DATE_STR}.csv")
        logger.debug(f"Saving extracted forecast to: {output_file.as_posix()}")
        df.to_csv(output_file, index=False)
        output_files.append(output_file)

    return output_files


def process_adecks(region_str, date_time=None, input_dir=None, models=None):
    paths = settings.get("paths")

    if not input_dir:
        input_dir = Path(paths.get("staging_dir"))
    else:
        input_dir = Path(input_dir)

    staging_dir = Path(paths.get("staging_dir")).joinpath("atcf", region_str)
    staging_dir.mkdir(parents=True, exist_ok=True)

    run_id = RUN_ID
    logger.debug(f"`run_id` set to: {run_id}")

    engine = create_engine(
        f"mysql+mysqlconnector://{settings.db.get('USER')}:{settings.db.get('PASS')}@{settings.db.get('HOST')}:{settings.db.get('PORT')}/{settings.db.get('SCHEMA')}"
    )
    Session = sessionmaker(engine)
    with Session() as session:
        region = session.query(Region).where(Region.short_name == region_str).one()
        data_source = session.query(DataSource).where(DataSource.short_name == "NHC").one()
        # process the defauls ATCF files so that each files is for a single forecast cycle
        proccessed_files = processADECKByCycle(
            input_dir, staging_dir, region, session, date_time=date_time, models=models
        )
        for file in proccessed_files:
            logger.debug(f"Processing {file.name}")
            df = pd.read_csv(file, parse_dates=["DATETIME"])
            for model_str, tracks in df.groupby("TECH"):
                model = session.query(Model).where(Model.short_name == model_str).one()

                logger.trace(f"Processing {model.short_name} forecast")
                # see if forecast record already exists
                forecast = (
                    session.query(Forecast)
                    .where(Forecast.data_source_id == data_source.id)
                    .where(Forecast.model_id == model.id)
                    .where(Forecast.region_id == region.id)
                    .where(Forecast.datetime_utc == pd.Timestamp(df.DATETIME.unique()[0]))
                    .one_or_none()
                )
                if not forecast:
                    # Create new entry
                    forecast = Forecast.from_dict(
                        dict(
                            data_source_id=data_source.id,
                            model_id=model.id,
                            region_id=region.id,
                            datetime_utc=pd.Timestamp(df.DATETIME.unique()[0]),
                            run_id=RUN_ID,
                        )
                    )
                    logger.debug(f"Adding forecast: {forecast!r}")
                    session.add(forecast)
                    # flush so we can populate the id and get the relationship links
                    session.flush()

                logger.info(
                    f"Processing tracks for forecast {forecast.model_short} [{forecast.datetime_utc.isoformat()}]"
                )
                for storm_id, steps in sorted(tracks.groupby("storm_id")):
                    storm = session.get(Storm, storm_id)
                    if storm is None:
                        logger.error(f"No storm in DB with storm_id: {storm_id}")
                        continue

                    logger.debug(f"Processing tracks for {storm.name}")
                    # all ATCF forecasts have ensemble_number == 1
                    track = (
                        session.query(Track)
                        .where(Track.storm_id == storm.id)
                        .where(Track.forecast_id == forecast.id)
                        .where(Track.ensemble_number == 1)
                        .one_or_none()
                    )
                    if not track:
                        track = Track.from_dict(
                            dict(storm_id=storm.id, forecast_id=forecast.id, ensemble_number=1, run_id=RUN_ID)
                        )
                        logger.debug(f"Adding track: {track!r}")
                        session.add(track)
                        # flush so we can populate the id and get the relationship links
                        session.flush()

                    for hour, rows in sorted(steps.groupby("TAU")):
                        step = (
                            session.query(Step).where(Step.track_id == track.id).where(Step.hour == hour).one_or_none()
                        )
                        if not step:
                            step_dict = atcf.stepFromDataFrame(rows, hour, track.id)
                            step = Step.from_dict(step_dict)
                            session.add(step)
                    logger.info(
                        f"Added {len(session.new)} steps for {storm.name}_{model.short_name}_{forecast.datetime_utc.isoformat()}"
                    )
                    # flush after adding all the
                    session.flush()
            # if len(session.dirty) >= 1 or len(session.new) >= 1:
            session.commit()

            file.unlink()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Process bdeck files and update existing storm records or insert new records"
    )
    parser.add_argument("region", type=str, choices=["AL", "EP"], help="NHC region to process")
    parser.add_argument(
        "-d",
        "--current_datetime",
        type=str,
        default="",
        help="Datetime use to determine if an observation is outdated or not ['yyyymmddHH']",
    )
    parser.add_argument("-i", "--input_dir", type=str, default=None, help="Directory where BDECK files can be found")
    parser.add_argument(
        "-m",
        "--models",
        default=None,
        action="append",
        type=str,
        choices=["CMC", "HMON", "HWRF", "OFCL", "LGEM", "SHIP", "AEMN", "AVNO", "EGRR"],
        help="Directory where BDECK files can be found",
    )
    parser.add_argument(
        "-l",
        "--loglevel",
        type=str,
        default="INFO",
        choices=["INFO", "DEBUG", "TRACE", "WARNING"],
        help="Level to set the logger to.",
    )

    args = parser.parse_args()

    config = {
        "handlers": [
            {
                "sink": sys.stdout,
                "format": "<g>{time:YYYY-MM-DD HH:mm:ss}</> | <lvl>{level: <10}</> | <c>{name}</>:<c>{function}</>:<c>{line}</> | <lvl>{message}</>",
                "backtrace": "True",
                "catch": "True",
                "level": args.loglevel,
                "enqueue": "True",
            }
        ]
    }
    logger.configure(**config)
    # add custom level to distinguish between different bdeck files
    logger.level(name="STORM", no=40, color="<light-magenta>")

    models = args.models
    if args.current_datetime == "":
        date_time = None
    else:
        date_time = datetime.strptime(args.current_datetime, "%Y%m%d%H").replace(tzinfo=timezone.utc)

    process_atcf_forecasts(args.region, date_time, args.input_dir, models=models)
    # process_cfan_forecasts(args.region, 'ECMWF', date_time)
