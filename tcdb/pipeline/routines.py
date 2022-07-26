from datetime import datetime, timedelta, timezone
from loguru import logger

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tcdb.models import Storm
from tcdb.config import settings


DATE_TIME = datetime.now(tz=timezone.utc)
RUN_ID = f"ROUTINE__{DATE_TIME.isoformat()}"

def updateActiveSystems(max_hours_old=12):
    hours_old = timedelta(hours=max_hours_old)
    engine = create_engine(
        f"mysql+mysqlconnector://{settings.db.get('USER')}:{settings.db.get('PASS')}@{settings.db.get('HOST')}:{settings.db.get('PORT')}/{settings.db.get('SCHEMA')}"
    )
    Session = sessionmaker(engine)
    current_datetime = datetime.now()
    with Session() as session:
        active_systems = session.query(Storm).where(Storm.status == "Active").all()
        for system in active_systems:
            dt = (current_datetime - system.end_date)
            if dt > hours_old:
                logger.info(f"{system.name} hasnt been updated in {int(dt.total_seconds() / 60 / 60)} hours. Updating status to `Archive`")
                system.status = "Archive"
                # update the run_id
                system.run_id = RUN_ID
                # flush the changes/additions to the DB
                session.commit()

def removeOldInvests(max_days_old=7):
    """
    Remove any records in the storm table that are still and invest (haven't transitions to a named storm) and are older
    than `max_days_old` days old.

    This is was necessary because we can't count on JTCW (and possibly NHC) to provide correct data for each update and there may be
    instances where start lats/lons change significantly which would create new storm records in the DB when it shouldnt. IMO this
    was the easiest fix.
    """
    invests_removed = 0
    days_old = timedelta(days=max_days_old)
    engine = create_engine(
        f"mysql+mysqlconnector://{settings.db.get('USER')}:{settings.db.get('PASS')}@{settings.db.get('HOST')}:{settings.db.get('PORT')}/{settings.db.get('SCHEMA')}"
    )
    Session = sessionmaker(engine)
    current_datetime = datetime.now()
    with Session() as session:
        invests = session.query(Storm).where(Storm.nhc_number >= 90).all()
        for system in invests:
            dt = (current_datetime - system.end_date)
            if dt > days_old:
                logger.info(f"Removing storm record {system.id} [{system.name}] along with {len(system._observations)} observations and {len(system._tracks)} tracks")
                session.delete(system)
                invests_removed += 1
                # flush the changes/additions to the DB
                session.commit()
    if invests_removed > 0:
        logger.info(f"Removed {invests_removed} outdated invest records")
    else:
        logger.indo(f"No invest records removed")