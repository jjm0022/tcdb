
from typing import Iterable
import xarray as xr
import pandas as pd
import numpy as np

from typing import List, Union
from loguru import logger
from dataclasses import dataclass
from datetime import datetime, timedelta

from tcdb.models import Storm
from tcdb.formatting import pretty_print

@dataclass
class StormDataset:
    id: int 
    annual_id: int
    nhc_number: int
    nhc_id: str
    start_date: datetime
    end_date: datetime
    status: str
    name: str 
    start_lat: float
    start_lon: float
    tracks: pd.DataFrame
    observations: pd.DataFrame

    def __repr__(self):
        summary = [f"<tcdb.datasets.StormDataset>"]
        include_keys = [key for key in self.__annotations__.keys() if key not in ['tracks', 'observations']]
        col_width = max([len(key) for key in include_keys]) + 3
        for col in include_keys:
            col_name = pretty_print(col, col_width)
            summary.append(f"{' '*4}{col_name}{self.__getattribute__(col)!r}")
        return "\n".join(summary) + "\n"
    
    @classmethod
    def fromStorm(
        cls,
        storm: Storm,
        track_models: List[str] = [],
        track_cycle_datetimes: List[datetime] = []):
        return cls(
            id=storm.id,
            annual_id=storm.annual_id,
            nhc_number=storm.nhc_number,
            nhc_id=storm.nhc_id,
            start_date=storm.start_date,
            end_date=storm.end_date,
            status=storm.status,
            name=storm.name,
            start_lat=storm.start_lat,
            start_lon=storm.start_lon,
            tracks = cls._buildTracks(storm, models=track_models, cycle_datetimes=track_cycle_datetimes),
            observations=cls._buildObservations(storm)
        )

    @classmethod
    def _buildTracks(
        cls,
        storm: Storm,
        models: List[str] = [],
        cycle_datetimes: List[datetime] = []) -> pd.DataFrame:

        steps = list()
        for track in storm._tracks:
            if len(models) > 0:
                if track.model not in models:
                    continue
            if len(cycle_datetimes) > 0:
                if track.init not in cycle_datetimes:
                    continue
            for step in track._steps:
                step_dict = step.to_dict()
                step_dict['model'] = track.model
                step_dict['init'] = track.init
                step_dict['valid'] = step.valid_utc
                steps.append(step_dict)
        df = pd.DataFrame.from_dict(steps)
        df = df.drop(labels=['id', 'run_id', 'last_update'], axis=1)
        return df
        
    @classmethod
    def _buildObservations(cls, storm: Storm) -> pd.DataFrame:
        obs = list()
        for ob in storm._observations:
            obs.append(ob.to_dict())
        df = pd.DataFrame.from_dict(obs)
        df = df.drop(labels=['id', 'storm_id', 'run_id', 'last_update'], axis=1)
        return df

