import argparse
import sys
import numpy as np
from loguru import logger
from pathlib import Path
from datetime import datetime, timedelta, timezone
import warnings

# warnings.filterwarnings("ignore")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import atcf
from tables import Region, Storm

from config import settings
from utils import greatCircleDistance


DATE_TIME = datetime.now(tz=timezone.utc)
DATE_STR = DATE_TIME.isoformat().split(".")[0]
RUN_ID = f"STORMS__{DATE_TIME.isoformat()}"


def findMatchingStorm(cursor, query, args, return_many=False):
    logger.trace(f"{query}")
    cursor.execute(query, args)
    rows = cursor.fetchall()
    if return_many:  # used when trying to match an old invest that has transitioned to a named storm
        if len(rows) == 0:
            return None
        else:
            logger.info(f"Found {len(rows)} storms with start_date [{rows[0][6]}]")
            return rows
    else:
        if len(rows) == 0:
            return None
        elif len(rows) == 1:
            return rows[0]
        else:
            raise ValueError(f"Too many rows ({len(rows)}) returned for query: {query} with arguments {args}")


def getClosestStorm(matched_storms, storm_dict):
    """Given a list of Storm records, this function will return the record of the storm that has the closest starting location
    to `storm_dict` that is within 100 nmi.

    Args:
        matched_storms (list[tables.Storm])
        storm_dict (dict)

    Returns:
        _type_: _description_
    """
    storm_list = matched_storms
    if len(storm_list) == 1:  # only 1 storm found with the same start_date
        matched_storm = storm_list[0]
        logger.info(
            f"Matched {storm_dict.get('name')} to {matched_storm.name} based on start_date [{matched_storm.start_date.isoformat()}]"
        )
    else:  # multiple storms found with same start_date
        # use starting location to determine the correct storm
        distance_dict = dict()
        for stm in matched_storms:
            distance = greatCircleDistance(
                storm_dict.get("start_lat"), storm_dict.get("start_lon"), stm.start_lat, stm.start_lon
            )
            distance_dict[distance] = stm
            logger.debug(f"{stm.id:02d}.{stm.name} started {distance:0.2f} nm from {storm_dict.get('name')}")
        # named storm with the shortest distance from the invest starting location
        min_distance = min(distance_dict.keys())
        if min_distance <= 100:  # closest storm must be within 100 nm to be considered a match
            matched_storm = distance_dict.get(min_distance)
        else:
            logger.warning(f"Matching storms do not start within 100 nm")
            matched_storm = None
    return matched_storm


def investSearch(session, storm_dict, date_time):
    """Search storms table for a named storm that can be associated with the invest `storm_dict`

    Args:
        session (sqlalchemy.orm.session.Session)
        storm_dict (dict): storm dict that was built using bdeck information
        date_time (datetime.datetime): Current datetime

    Returns:
        None: If a named storm was found that can be associated with the invest `storm_dict`

    """
    # If the end_date is older than 6 hours there's a chance the invest has transitioned to a named storm so we need to search the named
    # storms to see if we can find a match. There's a small chance multiple storms could have the same start_date so we need to use the
    # starting location to be certain when matching an invest to a named storm. If multiple named storms are found with the same start_date,
    # the named storm with a starting location that is closest to the invest starting location is used.
    if date_time - storm_dict.get("end_date") >= timedelta(hours=24):  # most likely an old invest
        logger.info(
            f"Most recent observation [{storm_dict.get('end_date').isoformat()}] for {storm_dict.get('name')} is older than 24 hours"
        )
        return None
    # if the end_date is less than 24
    else:
        # check to see if there's any named storms with the same start date so we don't add a new invest for a storm that has already transitioned
        named_storms = (
            session.query(Storm)
            .where(Storm.region_id == storm_dict.get("region_id"))
            .where(Storm.start_date == storm_dict.get("start_date"))
            .all()
        )

        # if there are any named storms with the same start date
        if len(named_storms) > 0:
            matched_storm = getClosestStorm(named_storms, storm_dict)
            if matched_storm is not None:  # if matched_storm is anything but None
                logger.info(f"{storm_dict.name} has transitioned to {matched_storm.id:02d}.{matched_storm.name}. ")
                return None  # We don't want to make any updates to invests that have transitioned to named storms

        # check to see if there's any existing invests with a matching start_date
        matched_storm = (
            session.query(Storm)
            .where(Storm.nhc_numer >= 70)
            .where(Storm.region_id == storm_dict.get("region_id"))
            .where(Storm.start_date == storm_dict.get("start_date"))
            .one_or_none()
        )

        if matched_storm is None:  # new invest
            matched_storm = Storm.from_dict(storm_dict)

    return matched_storm


def namedStormSearch(session, storm_dict, date_time):
    # Two Scenarios:
    # 1) storm already exists
    # 2) first observation after transition from invest
    matched_storm = session.query(Storm).where(Storm.nhc_id == storm_dict.get("nhc_id")).one_or_none()

    if matched_storm:  # easy scenario, storm with matching nhcId already exists
        logger.debug(f"{storm_dict.get('name')} matches with record {matched_storm.id} based on nhc_id search")
        # Assume the storm_dict has the most up-to-date information and use it to update the Storm object
        for key, value in storm_dict.items():
            if matched_storm.__getattribute__(key) != value:
                logger.debug(
                    f"Updating {matched_storm.__tablename__}.{key} for record {matched_storm.id} from {matched_storm.__getattribute__(key)} to {value}"
                )
                matched_storm.__setattr__(key, value)
    else:  # first observation after transition from invest
        # Need to find the invest in the same region with the same start_date
        matched_storms = (
            session.query(Storm)
            .where(Storm.nhc_number >= 70)
            .where(Storm.region_id == storm_dict.get("region_id"))
            .where(Storm.start_date == storm_dict.get("start_date"))
            .all()
        )

        if len(matched_storms) >= 1:  # found invest(s) with matching start_date
            matched_storm = getClosestStorm(matched_storms, storm_dict)
            if matched_storm is not None:  # if matched_storm is anything but None
                # Assume the storm_dict has the most up-to-date information and use it to update the Storm object
                for key, value in storm_dict.items():
                    if matched_storm.__getattribute__(key) != value:
                        logger.debug(
                            f"Updating {matched_storm.__tablename__}.{key} for record {matched_storm.id} from {matched_storm.__getattribute__(key)} to {value}"
                        )
                        matched_storm.__setattr__(key, value)
                logger.info(f"{matched_storm.id:02d}.{matched_storm.name} has transitioned to {storm_dict.get('name')}")
            else:
                logger.warning(
                    f"None of the matching storms had a close enough starting locaion... {matched_storms}\n\n{storm_dict}"
                )
        else:  # Must be populating the table from scratch !!!SHOULD NOT OCCUR WHEN RUNNING OPERATIONALLY!!!
            logger.debug(f"No matches found for {storm_dict.get('name')} after nhc_id and start_date search")
            matched_storm = Storm.from_dict(storm_dict)
    return matched_storm


def process_storms(region, date_time, staging_dir=None):
    """This script does multiple things:
    1) loop through bdeck files and match with existing storms in db
    2) if match is found check to see if any fields need to be updated
        2.1) If update is needed, save the fields that need to be updated to json
    3) if no match is found save the information to csv so the we can add it to the db

    Args:
        region ([type]): [description]
    """
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
        region_record = session.query(Region).where(Region.short_name == region).one()

        for file in sorted(staging_dir.glob(f"b{region.lower()}*.dat")):
            # build storm object from bdeck information
            storm_dict = atcf.toStormDict(file)
            storm_dict["region_id"] = region_record.id
            if date_time - storm_dict.get("end_date") <= timedelta(hours=16):
                storm_dict["status"] = "Active"
            else:
                storm_dict["status"] = "Archive"

            logger.log("STORM", f"---------- {storm_dict.get('name')} [{storm_dict.get('nhc_id')}] ----------")
            # if the storm is currently an invest we can't use nhc_id to search.
            if storm_dict.get("nhc_number") >= 70:
                storm = investSearch(session, storm_dict, date_time)
                if storm is None:  # old invest or invest that has transitioned to a named storm
                    continue
            else:
                storm = namedStormSearch(session, storm_dict, date_time)

            # give new storms an annual id
            if storm.annual_id is None:
                next_annual_id = (
                    session.query(Storm.annual_id)
                    .where(storm.season == storm.season)
                    .where(storm.region_id == storm.region_id)
                    .order_by(storm.annual_id)
                    .all()[-1][0]
                    + 1
                )
                logger.info(f"Assigning annual_id {next_annual_id} to {storm.name}")
                storm.annual_id = next_annual_id
                session.add(storm)

            # Check to see if the storm record will be updated or added to the DB
            # If it will be then update/add the RUN_ID
            if storm in session.dirty or storm in session.new:
                storm.run_id = RUN_ID

            # flush the changes/additions to the DB
            session.commit()
            # logger.debug(f"Updating the following storms: {session.dirty}")
            # logger.debug(f"Adding the following storms: {session.new}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Process bdeck files and update existing storm records or insert new records"
    )
    parser.add_argument("region", type=str, choices=["AL", "EP"], help="NHC region to process")
    parser.add_argument(
        "-d",
        "--current_datetime",
        type=str,
        default=DATE_TIME.strftime("%Y%m%d%H"),
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

    # date_time = datetime.strptime(args.current_datetime, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    date_time = datetime.strptime(args.current_datetime, "%Y%m%d%H")

    process_storms(args.region, date_time, staging_dir=args.input_dir)
