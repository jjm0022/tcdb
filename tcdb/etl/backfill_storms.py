# coding: utf-8
from IPython.core.debugger import set_trace
import numpy as np
import json
from loguru import logger
from pathlib import Path
from datetime import datetime, timezone

from sqlalchemy.orm import sessionmaker

from tcdb.etl import atcf
from tcdb.config import settings
from tcdb.models import (
    Storm,
    Region,
    Observation
)
from tcdb.models.connection import get_engine

DATE_TIME = datetime.now(tz=timezone.utc)
DATE_STR = DATE_TIME.isoformat().split(".")[0]
RUN_ID = f"BACKFILL__{DATE_TIME.isoformat()}"


def process_archive_data(data_dir):

    if isinstance(data_dir, str):
        data_dir = Path(data_dir)

    engine = get_engine()

    Session = sessionmaker(engine)
    with Session() as session:
        for bdeck_file in sorted(data_dir.glob("b*.dat.gz")):
            if not bdeck_file.is_file():
                continue
            file_name = bdeck_file.name # bal052021.dat.gz
            logger.info(f"Processing {file_name}")
            nhc_id = file_name.split('.')[0] # bal052021
            nhc_id = nhc_id[1:].upper() # AL052021
            region_str = nhc_id[:2]
            region_record = session.query(Region).where(Region.short_name == region_str).one_or_none()
            if region_record is None:
                logger.error(f"Unable to determine region for {file_name}")
                continue

            try:
                storm_dict = atcf.toStormDict(bdeck_file)
            except Exception as e:
                #logger.exception(e)
                continue

            if storm_dict is None:
                continue
            storm_dict["region_id"] = region_record.id
            storm_dict["status"] = "Archive"
            storm_dict["run_id"] = RUN_ID
            storm = Storm.from_dict(storm_dict)
            try:
                storm.annual_id = (
                    session.query(Storm.annual_id)
                    .where(Storm.season == storm.season)
                    .where(Storm.region_id == storm.region_id)
                    .order_by(Storm.annual_id)
                    .all()[-1][0]
                    + 1
                )
            except IndexError:
                storm.annual_id = 1
            
            logger.info(f"Adding: {storm}")
            
            session.add(storm)
            session.commit()
            

def process_20_21():
    archive_url = "https://ftp.nhc.noaa.gov/atcf/archive"
    active_url = "https://ftp.nhc.noaa.gov/atcf/btk"
    with open("../data/2020-2021_storms.json", "r") as j:
        storms = json.load(j)

    storm_list = list()
    for storm_dict in storms:
        cfan_id = storm_dict.get("cfan_id")
        nhc_id = storm_dict.get("nhc_id").lower()
        season = storm_dict.get("season")
        nhc_number = storm_dict.get("nhc_number")
        if nhc_number >= 70:
            continue

        if season == 2020:
            data_url = f"{archive_url}/{season}/b{nhc_id}.dat.gz"
            file_path = Path(f"/tmp/bdeck/{season}/b{nhc_id}.dat.gz")
        elif season == 2021:
            data_url = f"{active_url}/b{nhc_id}.dat"
            file_path = Path(f"/tmp/bdeck/{season}/b{nhc_id}.dat")

        if not file_path.exists():
            download(data_url, file_path)

        s = storm.stormFromBdeck(file_path)
        setattr(s, "cfan_id", cfan_id)
        setattr(s, "status", "Archive")
        setattr(s, "dag_id", RUN_ID)
        setattr(s, "region_id", 1)

        storm_list.append(s.to_json())

    return storm_list


if __name__ == "__main__":
    data_dir = Path('/tmp/ftp.nhc.noaa.gov/atcf/archive/2012/')
    process_archive_data(data_dir)

