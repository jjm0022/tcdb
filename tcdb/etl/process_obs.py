import argparse
import sys
from IPython.core.debugger import set_trace
from loguru import logger
from pathlib import Path
from datetime import datetime, timezone, timedelta
import warnings

warnings.filterwarnings("ignore")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tcdb.etl import atcf
from tcdb.config import settings
from tcdb.models import (
    Storm,
    Observation,
)

DATE_TIME = datetime.now(tz=timezone.utc)
DATE_STR = DATE_TIME.isoformat().split(".")[0]
RUN_ID = f"OBS__{DATE_TIME.isoformat()}"


def processObservations(region, date_time=None, staging_dir=None):
    paths = settings.get("paths")

    if not staging_dir:
        staging_dir = Path(paths.get("staging_dir"))
    else:
        staging_dir = Path(staging_dir)
    run_id = RUN_ID
    logger.info(f"`run_id` set to: {run_id}")

    engine = create_engine(
        f"mysql+mysqlconnector://{settings.db.get('USER')}:{settings.db.get('PASS')}@{settings.db.get('HOST')}:{settings.db.get('PORT')}/{settings.db.get('SCHEMA')}"
    )
    Session = sessionmaker(engine)
    with Session() as session:

        for file_path in sorted(staging_dir.glob(f"b{region.lower()}*.csv")):
            # if a date_time was provided, we only want to process files that have that datetime in them
            if date_time:
                if not atcf.contains_date(file_path, date_time):
                    logger.trace(f"{date_time} not in {file_path.as_posix()}")
                    continue
            logger.info(f"Processing {file_path.as_posix()}")
            storm_dict = atcf.toStormDict(file_path)
            # get the matching storm record
            storm = (
                session.query(Storm)
                .where(Storm.nhc_id == storm_dict.get("nhc_id"))
                .where(Storm.start_date == storm_dict.get("start_date"))
                .one_or_none()
            )

            # dont process observations if we can't associate them with an existing storm
            if storm is None:
                logger.info(f"No storm in DB matching {storm_dict.get('nhc_id')}. Skipping {file_path.name}")
                continue

            df = atcf.parse_bDeck(file_path)
            if date_time:
                df = df.loc[df.DATETIME == date_time]
            for _, obs in df.groupby("DATETIME"):
                ob_dict = atcf.observationDictFromDataFrame(obs, storm.id)
                observation = (
                    session.query(Observation)
                    .where(Observation.storm_id == ob_dict.get("storm_id"))
                    .where(Observation.datetime_utc == ob_dict.get("datetime_utc"))
                    .one_or_none()
                )
                if observation:  # If observation already exists, check to see if it needs to be updated
                    # Assume the ob_dict has the most up-to-date information and use it to update the Observation record
                    updated_keys = observation.updateFromDict(ob_dict)
                    if len(updated_keys) == 0:
                        logger.trace(f"No updates needed for observation {observation.id}")
                    if observation in session.dirty:
                        observation.run_id = RUN_ID
                else:
                    observation = Observation.from_dict(ob_dict)
                    observation.run_id = RUN_ID
                    session.add(observation)
                    logger.info(f"Adding new observation record for {storm.name} [{observation.datetime_utc.isoformat()}]")

            session.commit()


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
        "-l",
        "--loglevel",
        type=str,
        default="INFO",
        choices=["INFO", "DEBUG", "TRACE"],
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

    if args.current_datetime == "":
        date_time = None
    else:
        # date_time = datetime.strptime(args.current_datetime, "%Y%m%d%H").replace(tzinfo=timezone.utc)
        date_time = datetime.strptime(args.current_datetime, "%Y%m%d%H")
    processObservations(args.region, date_time, staging_dir=args.input_dir)
