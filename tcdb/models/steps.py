from datetime import timedelta
from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import Column, Integer, Float, String, TIMESTAMP, text

from tcdb.models.base import Base, DefaultTable
import tcdb.validation as val


class Step(Base, DefaultTable):
    __tablename__ = "steps"

    id = Column(Integer, primary_key=True, autoincrement=True)
    track_id = Column(Integer, ForeignKey("tracks.id"), nullable=False)
    hour = Column(Integer, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    intensity_kts = Column(Float, nullable=False)
    mslp_mb = Column(Float, nullable=False)
    #r34_ne = Column(Integer)
    #r34_se = Column(Integer)
    #r34_sw = Column(Integer)
    #r34_nw = Column(Integer)
    #r50_ne = Column(Integer)
    #r50_se = Column(Integer)
    #r50_sw = Column(Integer)
    #r50_nw = Column(Integer)
    #r64_ne = Column(Integer)
    #r64_se = Column(Integer)
    #r64_sw = Column(Integer)
    #r64_nw = Column(Integer)
    run_id = Column(String(255), nullable=False)
    last_update = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    __table_args__ = (UniqueConstraint("track_id", "hour", name="steps_index"),)

    _track = relationship("Track", back_populates="_steps")

    @classmethod
    def from_dict(cls, d):
        return cls(
            id=val.ensure_int_none(d.get("id"), "id"),
            track_id=val.ensure_int(d.get("track_id"), "track_id"),
            hour=val.validate_forecast_step(d.get("hour"), raise_on_fail=True),
            latitude=val.validate_latitude(d.get("latitude"), raise_on_fail=True),
            longitude=val.validate_longitude(d.get("longitude"), raise_on_fail=True),
            intensity_kts=val.validate_velocity(d.get("intensity_kts"), raise_on_fail=True),
            mslp_mb=val.validate_pressure(d.get("mslp_mb"), raise_on_fail=True),
            #r34_ne=val.validate_distance(d.get("r34_ne")),
            #r34_se=val.validate_distance(d.get("r34_se")),
            #r34_sw=val.validate_distance(d.get("r34_sw")),
            #r34_nw=val.validate_distance(d.get("r34_nw")),
            #r50_ne=val.validate_distance(d.get("r50_ne")),
            #r50_se=val.validate_distance(d.get("r50_se")),
            #r50_sw=val.validate_distance(d.get("r50_sw")),
            #r50_nw=val.validate_distance(d.get("r50_nw")),
            #r64_ne=val.validate_distance(d.get("r64_ne")),
            #r64_se=val.validate_distance(d.get("r64_se")),
            #r64_sw=val.validate_distance(d.get("r64_sw")),
            #r64_nw=val.validate_distance(d.get("r64_nw")),
            run_id=val.ensure_str(d.get("run_id", ""), "run_id"),
        )

    @property
    def valid_utc(self):
        return self._track._forecast.datetime_utc + timedelta(hours=self.hour)
