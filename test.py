# coding: utf-8
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from tcdb.config import settings

from tcdb.models import (
    Storm,
    Track,
    Observation,
    Step,
    Model,
    Forecast,
    DataSource,
    Region
)

from tcdb.datasets import StormDataset

def export(session):

    obs = session.query(Observation).all()
    storms = session.query(Storm).all()
    tracks = session.query(Track).all()
    steps = session.query(Step).all()
    models = session.query(Model).all()
    forecasts = session.query(Forecast).all()
    data_sources = session.query(DataSource).all()
    regions = session.query(Region).all()

    for table in [obs, storms, tracks, steps, models, forecasts, data_sources, regions]:
        table_name = table[0].__tablename__
        print(table_name)
        table_list = list()

        for row in table:
            table_list.append(row.to_dict(serializable=True))

        with open(f"/Users/jmiller/Work/tcdb/data/json/{table_name}.json", 'w') as j:
            json.dump(table_list, j, indent=True)

if __name__ == "__main__":

    engine = create_engine(f"mysql+mysqlconnector://{settings.db.get('USER')}:{settings.db.get('PASS')}@{settings.db.get('HOST')}:{settings.db.get('PORT')}/{settings.db.get('SCHEMA')}")
    Session = sessionmaker(bind=engine)
    session = Session()

    storm = session.query(Storm).where(Storm.id == 8).one() 
    sd = StormDataset.fromStorm(storm)