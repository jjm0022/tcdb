
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
