import pathlib
from pathlib import Path
from loguru import logger
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tcdb.config import settings
from tcdb.models import (
    Region,
    Storm,
    Observation,
    Forecast,
    Step,
    Track,
)

def getEngine():

    engine = create_engine(
        f"mysql+mysqlconnector://{settings.db.get('USER')}:{settings.db.get('PASS')}@{settings.db.get('HOST')}:{settings.db.get('PORT')}/{settings.db.get('SCHEMA')}"
    )
    return engine

def inferStormFromAdeck(adeck_path):
    r"""Given a path to an adeck file (in standard naming format) this functino will return a storm record from the database if one exists
    If multiple storms are found None is returned
    If no storm is found, None is returned

    Args:
        adeck_path (pathlib.Path): Path to the adeck file 

    Returns:
        tcdb.models.Storm: If a matching storm is identified in the DB_type
        None: If no match is found or multiple matches are found
    """
    assert isinstance(adeck_path, pathlib.Path), f"expected `adeck_path` to be an instance of pathlib.Path not {type(adeck_path)}"

    file_name = adeck_path.name
    basin = file_name[1:3]
    nhc_number = int(file_name[3:5])
    season = int(file_name.split('.')[0][5:])

    Session = sessionmaker(getEngine())
    with Session() as session:
        region = session.query(Region).where(Region.short_name == basin.upper()).one()
        storms = (
            session.query(Storm)
                .where(Storm.region_id == region.id)
                .where(Storm.season == season)
                .where(Storm.nhc_number == nhc_number).all()
        )
        if len(storms) == 0:
            logger.debug(f"No storm associated with {adeck_path.as_posix()}")
            return None
        elif len(storms) == 1:
            storm = storms[0]
            logger.info(f"{adeck_path.name} is associated with storm {storm.id} [{storm.name}]")
            return storm
        else:
            logger.warning(f"{adeck_path.name} is associated with the following {len(storms)} storms:")
            for storm in storms:
                #logger.warning(f"{storm.id} [{storm.name} - {storm.nhc_id}] ")
                logger.warning(storm)
            return None

def getRegionShort(region_id):

    Session = sessionmaker(getEngine())
    with Session() as session:
        return session.query(Region).where(Region.id == region_id).one().short_name
