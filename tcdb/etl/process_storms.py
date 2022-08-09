import argparse
import sys
from loguru import logger
from pathlib import Path
from datetime import datetime, timedelta, timezone
import warnings

# warnings.filterwarnings("ignore")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tcdb.etl import atcf
from tcdb.models import Region, Storm
from tcdb.config import settings
from tcdb.utils import greatCircleDistance


DATE_TIME = datetime.now(tz=timezone.utc)
DATE_STR = DATE_TIME.isoformat().split(".")[0]
RUN_ID = f"STORMS__{DATE_TIME.isoformat()}"


def getClosestStorm(matched_storms, storm_dict):
    """Given a list of Storm records, this function will return the record of the storm that has the closest starting location
    to `storm_dict` that is within 100 nmi.

    Args:
        matched_storms (list[tables.Storm])
        storm_dict (dict): dictionary with the keys "name", "start_lat", and "start_lon"

    Returns:
        tcdb.models.Storm: The storm closest to the center of `storm_dict` 
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
                storm_dict.get("start_lat"),
                storm_dict.get("start_lon"),
                stm.start_lat,
                stm.start_lon
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
    # If the end_date is older than 24 hours there's a chance the invest has transitioned to a named storm so we need to search the named
    # storms to see if we can find a match. There's a small chance multiple storms could have the same start_date so we need to use the
    # starting location to be certain when matching an invest to a named storm. If multiple named storms are found with the same start_date,
    # the named storm with a starting location that is closest to the invest starting location is used.
    if date_time - storm_dict.get("end_date") >= timedelta(hours=24):  # most likely an old invest
        logger.info(f"Most recent observation [{storm_dict.get('end_date').isoformat()}] for {storm_dict.get('name')} is older than 24 hours")
        return None
    # if the end_date is less than 24
    else:
        # check to see if there's any named storms with the same start date so we don't add a new invest for a storm that has already transitioned
        named_storms = (
            session.query(Storm)
            .where(Storm.nhc_number <= 50)
            .where(Storm.region_id == storm_dict.get("region_id"))
            .where(Storm.start_date == storm_dict.get("start_date"))
            .all()
        )
        # if there are any named storms with the same start date
        if len(named_storms) > 0:
            matched_storm = getClosestStorm(named_storms, storm_dict)
            if matched_storm is not None:  # if matched_storm is anything but None
                logger.info(f"{storm_dict.get('name')} has transitioned to {matched_storm.id:02d}.{matched_storm.name}")
                return None  # We don't want to make any updates to invests that have transitioned to named storms

        # check to see if there's any existing invests with a matching start_date
        # apparently JTWC doesn't give a shit and the first couple of updates for an invest the starting information is capable
        # of changing significantly so I'm starting to think it will be better to just search for nhc_id and then make sure the
        # start date is within 24 hours to match because we can't count on the start lat/lon or start date to be correct in the first
        # update for a storm
        matched_storms = (
            session.query(Storm)
            .where(Storm.nhc_id == storm_dict.get("nhc_id"))
            .all()
        )
        matched_storm = None
        for _storm in matched_storms:
            # hour difference in start_dates
            td = abs((_storm.start_date - storm_dict.get('start_date')).total_seconds() / 60 / 60)
            if td > 24:
                continue
            else:
                matched_storm = _storm
                break

        if matched_storm is None:  # new invest
            matched_storm = Storm.from_dict(storm_dict)
        else:
            # see if anything needs to be updated
            updated_keys = matched_storm.updateFromDict(storm_dict)

    return matched_storm


def namedStormSearch(session, storm_dict):
    # Two Scenarios:
    # 1) storm already exists
    # 2) first observation after transition from invest
    matched_storm = session.query(Storm).where(Storm.nhc_id == storm_dict.get("nhc_id")).one_or_none()

    if matched_storm:  # easy scenario, storm with matching nhcId already exists
        # assume that we should only update the end_date if the new date is greater than the current date.
        # this should keep from accidentally running an old file and updating to an older date accidentally
        if matched_storm.end_date > storm_dict.get('end_date'):
            logger.warning(f"Current `end_date` [{matched_storm.end_date.isoformat()}) is newer than the proposed update ({storm_dict.get('end_date').isoformat()}). Returning the existing DB entry")
            return matched_storm
        logger.debug(f"{storm_dict.get('name')} matches with record {matched_storm.id} based on nhc_id search")
        # Assume the storm_dict has the most up-to-date information and use it to update the Storm object
        updated_keys = matched_storm.updateFromDict(storm_dict)
        if len(updated_keys) == 0:
            logger.info(f"No updates needed for {matched_storm.id} [{matched_storm.name}]")
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
                if matched_storm.name != storm_dict.get('name'):
                    logger.info(f"{matched_storm.id} [{matched_storm.name}] has transitioned to {storm_dict.get('name')}")
                # Assume the storm_dict has the most up-to-date information and use it to update the Storm object
                updated_keys = matched_storm.updateFromDict(storm_dict)
                if len(updated_keys) == 0:
                    logger.info(f"No updates needed for {matched_storm.id} [{matched_storm.name}]")
            else:
                logger.warning(f"None of the matching storms had a close enough starting locaion... {matched_storms}\n\n{storm_dict}")
        else:  # Must be populating the table from scratch !!!SHOULD NOT OCCUR WHEN RUNNING OPERATIONALLY!!!
            logger.debug(f"No matches found for {storm_dict.get('name')} after nhc_id and start_date search")
            matched_storm = Storm.from_dict(storm_dict)
    return matched_storm


def processStorms(region, date_time, staging_dir=None):
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
        # using sorted ensures we process any invest files after named storms
        for file in sorted(staging_dir.glob(f"b{region.lower()}*.csv")):
            # build storm object from bdeck information
            try:
                storm_dict = atcf.toStormDict(file)
            except:
                logger.error(f"Unable to parse {file.as_posix()}")
                continue
            storm_dict["region_id"] = region_record.id
            if date_time - storm_dict.get("end_date") <= timedelta(hours=16):
                storm_dict["status"] = "Active"
            else:
                storm_dict["status"] = "Archive"

            logger.info(f"---------- {storm_dict.get('name')} [{storm_dict.get('nhc_id')}] ----------")
            # if the storm is currently an invest we can't use nhc_id to search.
            if storm_dict.get("nhc_number") >= 90:
                storm = investSearch(session, storm_dict, date_time)
                if storm is None:  # old invest or invest that has transitioned to a named storm
                    continue
            else:
                storm = namedStormSearch(session, storm_dict)

            # give new storms an annual id
            if storm.annual_id is None:
                try:
                    next_annual_id = (
                        session.query(Storm.annual_id)
                        .where(Storm.season == storm.season)
                        .where(Storm.region_id == storm.region_id)
                        .order_by(Storm.annual_id)
                        .all()[-1][0]
                        + 1
                    )
                except IndexError: # means this is the first storm of the season
                    next_annual_id = 1

                logger.info(f"Assigning annual_id {next_annual_id} to {storm.name}")
                storm.annual_id = next_annual_id
                session.add(storm)

            # Check to see if the storm record will be updated or added to the DB
            # If it will be then update/add the RUN_ID
            if storm in session.dirty or storm in session.new:
                storm.run_id = RUN_ID

            # flush the changes/additions to the DB
            session.commit()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Process bdeck files and update existing storm records or insert new records"
    )
    parser.add_argument("region", type=str, choices=["AL", "EP", "WP", "CP", "IO", "SH"], help="NHC region to process")
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

    # date_time = datetime.strptime(args.current_datetime, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    date_time = datetime.strptime(args.current_datetime, "%Y%m%d%H")

    if args.input_dir is None:
        staging_dir = Path(settings.paths.staging_dir).joinpath('bdeck')
    else:
        staging_dir = Path(args.input_dir)


    processStorms(args.region, date_time, staging_dir=staging_dir)
