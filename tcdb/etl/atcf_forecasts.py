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
from tcdb.models import Forecast, Track, Step, DataSource, Model, Storm
from tcdb.etl import syntracks, invest

DATE_TIME = datetime.now(tz=timezone.utc)
DATE_STR = DATE_TIME.isoformat().split(".")[0]
RUN_ID = f"ATCF__{DATE_TIME.isoformat()}"



def process_adecks(file_list, remove=True):
    """Load ATCF track data from a csv and save the data to the database

    Args:
        file_list (list[pathlib.Path]): list of paths to ATCF track files
        remove (bool, optional): Remove the track file after processing. Default 
    """
    paths = settings.get("paths")
    # variables to keep track of additions and changes to the DB 
    forecasts_added = 0
    tracks_added = 0
    steps_added = 0
    steps_updated = 0


    run_id = RUN_ID
    logger.trace(f"`run_id` set to: {run_id}")

    engine = create_engine(
        f"mysql+mysqlconnector://{settings.db.get('USER')}:{settings.db.get('PASS')}@{settings.db.get('HOST')}:{settings.db.get('PORT')}/{settings.db.get('SCHEMA')}"
    )
    Session = sessionmaker(engine)
    with Session() as session:
        for ind, file in enumerate(file_list):
            logger.trace(f"Processing {file.name}")
            # https://docs.sqlalchemy.org/en/14/orm/session_basics.html#framing-out-a-begin-commit-rollback-block
            with session.begin():
                # get storm information 
                storm_id = int(file.name.split('-')[1])
                storm = session.query(Storm).where(Storm.id == storm_id).one()
                if ind == 0:
                    logger.info(f"Loading track files for {storm.name} into database")

                date_time = datetime.strptime(file.stem.split('_')[-1], '%Y%m%d%H')

                # get data source information
                region = storm._region
                if region.short_name.lower() in ['al', 'ep', 'cp']:
                    data_source = session.query(DataSource).where(DataSource.short_name == "NHC").one()
                else:
                    data_source = session.query(DataSource).where(DataSource.short_name == "JTWC").one()
                    
                # get model information            
                model_str = file.name.split('_')[1]
                model = session.query(Model).where(Model.short_name == model_str).one()

                # parse the adeck file
                df = pd.read_csv(file) 

                # see if forecast record already exists
                forecast = (
                    session.query(Forecast)
                    .where(Forecast.data_source_id == data_source.id)
                    .where(Forecast.model_id == model.id)
                    .where(Forecast.region_id == region.id)
                    .where(Forecast.datetime_utc == date_time)
                    .one_or_none()
                )
                if forecast is None:
                    # Create new entry
                    forecast = Forecast.from_dict(
                        dict(
                            data_source_id=data_source.id,
                            model_id=model.id,
                            region_id=region.id,
                            datetime_utc=date_time,
                            run_id=RUN_ID,
                        )
                    )
                    logger.trace(f"Adding forecast:\n{forecast!r}")
                    session.add(forecast)
                    # flush so we can populate the id and get the relationship links
                    session.flush()
                    forecasts_added += 1

                logger.trace(f"Processing tracks for forecast {forecast.model_short} [{forecast.datetime_utc.isoformat()}]")
                track = (
                    session.query(Track)
                    .where(Track.storm_id == storm.id)
                    .where(Track.forecast_id == forecast.id)
                    .where(Track.ensemble_number == 1) # all ATCF forecasts have ensemble_number == 1
                    .one_or_none()
                )
                if track is None:
                    track = Track.from_dict(
                        dict(storm_id=storm.id, forecast_id=forecast.id, ensemble_number=1, run_id=RUN_ID)
                    )
                    logger.trace(f"Adding track:\n{track!r}")
                    session.add(track)
                    # flush so we can populate the id and get the relationship links
                    session.flush()
                    tracks_added += 1

                # iterate through forecast steps and add to DB
                for hour, rows in sorted(df.groupby("TAU")):
                    step = session.query(Step).where(Step.track_id == track.id).where(Step.hour == hour).one_or_none()
                    if step is None:
                        step = Step.from_dict(atcf.stepFromDataFrame(rows, hour, track.id))
                        step.run_id = RUN_ID
                        session.add(step)
                        steps_added += 1
                    else:
                        # see if anything needs to be updated
                        tmp_dict = atcf.stepFromDataFrame(rows, hour, track.id)
                        updated_keys = step.updateFromDict(tmp_dict, check_only=False)
                        if len(updated_keys) > 0:
                            steps_updated +=1
                # flush after adding all the steps
                session.flush()
                if remove:
                    file.unlink()
            session.commit()
        logger.info(f"Summary for storm {storm.id} [{storm.name}]")
        logger.info(f"\t Added {forecasts_added} new forecasts")
        logger.info(f"\t Added {tracks_added} new tracks")
        logger.info(f"\t Added {steps_added} new steps")
        logger.info(f"\t Updated {steps_updated} new steps")



if __name__ == "__main__":

    config = {
        "handlers": [
            {
                "sink": sys.stdout,
                "format": "<g>{time:YYYY-MM-DD HH:mm:ss}</> | <lvl>{level: <10}</> | <c>{name}</>:<c>{function}</>:<c>{line}</> | <lvl>{message}</>",
                "backtrace": "True",
                "catch": "True",
                "level": "DEBUG",
                "enqueue": "True",
            }
        ]
    }
    logger.configure(**config)

    #process_atcf_forecasts(args.region, date_time, args.input_dir, models=models)
    dir = Path('/Work_Data/tcdb/data/lake/atcf/al/adeck/2022/01/')
    l = dir.glob("*OFCL*")
    process_adecks(l, remove=False)