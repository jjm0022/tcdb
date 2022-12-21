from collections import namedtuple
from datetime import datetime
from loguru import logger
from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import Column, Integer, Float, String, DateTime, TIMESTAMP, text

from tcdb.models.base import Base, DefaultTable
import tcdb.validation as val


class Storm(Base, DefaultTable):
    __tablename__ = "storms"

    id = Column(Integer, primary_key=True, autoincrement=True)
    annual_id = Column(Integer, nullable=False)
    region_id = Column(Integer, ForeignKey("regions.id"), nullable=False)
    nhc_number = Column(Integer, nullable=False)
    nhc_id = Column(String(10), nullable=False)
    season = Column(Integer, nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime)
    status = Column(String(10), nullable=False)
    name = Column(String(25), nullable=False)
    start_lat = Column(Float, nullable=False)
    start_lon = Column(Float, nullable=False)
    run_id = Column(String(255), nullable=False)
    last_update = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    _tracks = relationship("Track", order_by="Track.id", back_populates="_storm", cascade="all, delete-orphan")
    _observations = relationship("Observation", order_by="Observation.datetime_utc", back_populates="_storm", cascade="all, delete-orphan")
    _region = relationship("Region", back_populates="_storms")

    __table_args__ = (UniqueConstraint("start_date", "nhc_id", name="storms_index"),)

    @classmethod
    def from_dict(cls, d):
        return cls(
            id=val.ensure_int_none(d.get("id"), "id"),
            annual_id=val.ensure_int_none(d.get("annual_id"), "annual_id"),
            region_id=val.ensure_int(d.get("region_id"), "region_id"),
            nhc_number=val.ensure_int(d.get("nhc_number"), "nhc_number"),
            nhc_id=val.ensure_str(d.get("nhc_id"), "nhc_id"),
            season=val.ensure_int(d.get("season"), "season"),
            start_date=val.ensure_datetime(d.get("start_date"), "start_date"),
            end_date=d.get("end_date"),
            status=val.ensure_str(d.get("status"), "status"),
            name=val.ensure_str(d.get("name"), "name"),
            start_lat=val.validate_latitude(d.get("start_lat"), raise_on_fail=True),
            start_lon=val.validate_longitude(d.get("start_lon"), raise_on_fail=True),
            run_id=val.ensure_str(d.get("run_id", ""), "run_id"),
        )

    @property
    def region_short(self):
        return self._region.short_name

    @property
    def region_long(self):
        return self._region.long_name

    @property
    def region_char(self):
        return self._region.region_char

    @property
    def num_observatoins(self):
        return len(self._observations)

    def get_invest_info(self, date_time: datetime) -> namedtuple:
        """Given a date_time, this function will return a named tupl with all the information a CFAN invest would have

        Args:
            date_time (datetime.datetime): _description_
        """
        ob = list(filter(lambda x: x.datetime_utc == date_time, self._observations))
        if len(ob) == 0:
            logger.warning(f"Unable to find an observation for the provided date_time: {date_time.isoformat()}")
            return None
        if len(ob) > 1:
            raise RuntimeError(f"Too many observations returned for date_time: {date_time.isoformat()}")

        ob = ob[0]
        Inv = namedtuple("Invest", "name annual_id nhc_number nhc_id valid latitude longitude wind mslp")

        inv = Inv(
            name=self.name,
            annual_id=self.annual_id,
            nhc_number=self.nhc_number,
            nhc_id=self.nhc_id,
            valid=date_time,
            latitude=ob.latitude,
            longitude=ob.longitude,
            wind=ob.intensity_kts,
            mslp=ob.mslp_mb,
        )

        return inv
