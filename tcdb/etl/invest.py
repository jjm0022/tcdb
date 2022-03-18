import numpy as np
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from loguru import logger

from tcdb.config import settings


@dataclass
class Invest:
    valid: datetime = np.datetime64("NaT")
    storm_name: str = "NaN"
    annual_id: int = np.nan
    cfan_id: int = np.nan
    nhc_number: int = np.nan
    lat: float = np.nan
    lon: float = np.nan
    wind: float = np.nan
    mslp: float = np.nan
    file_path: str = "NaN"


def getInvestDict(p, annual_id=None, region="AL"):

    with open(p, "r") as t:
        lines = t.readlines()

    invests = dict()
    for line in lines:
        el = line.split(" ")
        inv = Invest(
            valid=datetime.strptime(el[5], "%Y%m%d%H").replace(tzinfo=timezone.utc),
            storm_name=el[3],
            annual_id=int(el[0]),
            cfan_id=int(el[0]),
            nhc_number=int(el[1]),
            lat=float(el[6]),
            lon=float(el[7]),
            wind=float(el[8]),
            mslp=float(el[9]),
            file_path=p.as_posix(),
        )
        invests[inv.cfan_id] = inv

    if annual_id:
        return invests.get(annual_id)
    else:
        return invests
