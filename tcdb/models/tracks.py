from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import Column, Integer, String, TIMESTAMP, text

from tcdb.models.base import Base, DefaultTable
import tcdb.validation as val


class Track(Base, DefaultTable):
    __tablename__ = "tracks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    storm_id = Column(Integer, ForeignKey("storms.id"), nullable=False)
    forecast_id = Column(Integer, ForeignKey("forecasts.id"), nullable=False)
    ensemble_number = Column(Integer, nullable=False)
    run_id = Column(String(255), nullable=False)
    last_update = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    __table_args__ = (UniqueConstraint("forecast_id", "storm_id", "ensemble_number", name="tracks_index"),)

    _steps = relationship("Step", order_by="Step.id", back_populates="_track")
    _storm = relationship("Storm", back_populates="_tracks")
    _forecast = relationship("Forecast", back_populates="_tracks")

    @classmethod
    def from_dict(cls, d):
        return cls(
            id=val.ensure_int_none(d.get("id"), "id"),
            storm_id=val.ensure_int(d.get("storm_id"), "storm_id"),
            forecast_id=val.ensure_int(d.get("forecast_id"), "forecast_id"),
            ensemble_number=val.ensure_int(d.get("ensemble_number"), "ensemble_number"),
            run_id=val.ensure_str(d.get("run_id", ""), "run_id"),
        )

    @property
    def num_steps(self):
        return len(self._steps)

    @property
    def storm_name(self):
        return self._storm.name

    @property
    def forecast_key(self):
        return f"{self._forecast.model_short} {self._forecast.datetime_utc.isoformat()}"
