from __future__ import annotations # https://stackoverflow.com/a/33533514
import json
import numpy as np
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from loguru import logger
from typing import Union

from sqlalchemy.orm import sessionmaker

from tcdb.config import settings
from tcdb.utils import is_serializable, json_encode
from tcdb.formatting import pretty_print
from tcdb.models import (
    database,
    Observation,
    Storm
)

@dataclass
class Invest:
    id: int
    annual_id: int
    nhc_number: int
    name: str
    basin: str
    date_time: datetime = np.datetime64("NaT")
    latitude: float = np.nan
    longitude: float = np.nan
    wind: float = np.nan
    mslp: float = np.nan

    def __repr__(self):
        summary = [f"<tcdb.models.Invest>"]
        include_keys = [key for key in self.__annotations__.keys()]
        col_width = max([len(key) for key in include_keys]) + 3
        for col in include_keys:
            col_name = pretty_print(col, col_width)
            summary.append(f"{' '*4}{col_name}{self.__getattribute__(col)!r}")
        return "\n".join(summary) + "\n"

    @classmethod
    def fromStorm(
        cls,
        storm: Storm,
        date_time: Union[datetime, None] = None) -> Invest:
        r"""Creates an Invest object from a tcdb.models.Storm object

        By default it will populate observation attributes using the most recent observation (end_date)

        Args:
            storm (tcdb.models.Storm): Storm object that the Invest will be created from 
            date_time (datetime.datetime): Observation date_time used to populate the observation-based attributes. Default
            is None. If no date_time is provided storm.end_date will be used.

        Returns:
            Invest
        """
        if date_time is None:
            # use the most recent observation by default
            date_time = storm.end_date

        if storm.start_date <= date_time <= storm.end_date:
            observation = [ob for ob in storm._observations if ob.datetime_utc == date_time][0]
        else:
            logger.error(f"{storm.name} as no observation on {date_time.isoformat()}")
            raise ValueError(f"'date_time' must be within the storms 'start_date' [{storm.start_date.isoformat()}] and 'end_date' [{storm.end_date.isoformat()}]")
        return cls(
            id=storm.id,
            annual_id=storm.annual_id,
            nhc_number=storm.nhc_number,
            name=storm.name,
            basin=storm.region_short.lower(),
            date_time=observation.datetime_utc,
            latitude=observation.latitude,
            longitude=observation.longitude,
            wind=observation.intensity_kts,
            mslp=observation.mslp_mb
        )

    @classmethod
    def fromObservation(
        cls,
        observation: Observation) -> Invest:
        r"""Creates an Invest object from a tcdb.models.Observation object.

        NOTE: The sqlalchemy Session connected to the 'observation' object must still be active. 

        Args:
            observation (Observation): Observation object used to initialize the Invest object

        Returns:
            Invest: _description_
        """
        return cls(
            id=observation.storm_id,
            annual_id=observation._storm.annual_id,
            nhc_number=observation._storm.nhc_number,
            name=observation._storm.name,
            basin=observation._storm.region_short.lower(),
            date_time=observation.datetime_utc,
            latitude=observation.latitude,
            longitude=observation.longitude,
            wind=observation.intensity_kts,
            mslp=observation.mslp_mb
        )

    def updateDatetime(
        self,
        date_time: datetime,
        inplace: bool =False) -> Union[bool, Invest]:
        r"""Update the Invest instance so the the attributes reflect the observation information from
        the provided date_time

        Args:
            date_time (datetime): Datetime of the observation
            inplace (bool, optional): If True, the current Invest instance is updated and returned. If False, a 
            new Invest instance is returned. Defaults to False.

        Returns:
            (bool | Invest): Returns a bool if 'inplace' is True. Returns a new instance of Invest if 'inplace' is False 
        """
        Session = sessionmaker(database.getEngine())
        with Session() as session:
            observation = session.query(Observation)\
                .where(Observation.storm_id == self.id)\
                .where(Observation.datetime_utc == date_time).one_or_none()
            if observation is None:
                logger.error(f"No observation record for storm {self.id} [{self.name}] at {date_time.isoformat()}")
                if inplace:
                    return False
                else:
                    return self
            else:
                if inplace:
                    self.date_time = observation.datetime_utc 
                    self.latitude = observation.latitude
                    self.longitude = observation.longitude
                    self.wind = observation.intensity_kts
                    self.mslp = observation.mslp_mb
                    return True
                else:
                    return Invest.fromObservation(observation)

    def dict(self):
        r"""Convert the Invest instance to a dict

        Returns:
            dict
        """
        out_dict = dict()
        for key in self.__dataclass_fields__.keys():
            out_dict[key] = self.__getattribute__(key)
        return out_dict

    def json(self):
        r"""Convert a Invest instance to a dict that meets json specs (datetime objects are converted to strings)

        Raises:
            ValueError: Raised for types that I'm not sure what to convert to

        Returns:
            dict
        """
        inv_dict = self.dict()

        for key, value in inv_dict.items():
            if is_serializable(value):
                continue
            else:
                inv_dict[key] = json_encode(value)
        return inv_dict 

    def save(
        self,
        path: Path=None,
        indent: int=4):
        r"""Save the Invest instance to a json.

        Args:
            path (pathlib.Path, optional): Local file path where json will be saved. If None, the 'invest_file_template' entry in the settings
                will be used. If the file path already exists, and the current Invest is not already listed in the json, the Invest attributes
                will be added to the file. If the Invest is already in the json, the information in the json will be updated using the current
                instance. Defaults to None.
            indent (int, optional): Number of spaces to indent the json file. Defaults to 4.
        """
        if path is None:
            info_dict = {
                'datetime': self.date_time.strftime('%Y%m%d%H'),
                'basin': self.basin.lower()
            }
            path = Path(settings.paths.data_lake).joinpath('inv')
            path.mkdir(parents=True, exist_ok=True)
            path  = path.joinpath(settings.paths.invest_file_template.format_map(info_dict))

        if isinstance(path, str):
            path = Path(path)
        # add to file if exists
        if path.exists():
            logger.trace("Invest path already exists. Adding/Updating current Invest Instance")
            with open(path, 'r') as j:
                output = json.load(j)
                output[str(self.id)] = self.json()
        else:
            output = {str(self.id): self.json()}
        logger.info(f"Saving Invest data to: {path.as_posix()}")
        with open(path, 'w') as j:
            json.dump(output, j, indent=indent)
