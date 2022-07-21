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
