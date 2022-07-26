from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import Column, Integer, String, DateTime, TIMESTAMP, text

from tcdb.models.base import Base, DefaultTable
import tcdb.validation as val


class Forecast(Base, DefaultTable):
    __tablename__ = "forecasts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    data_source_id = Column(Integer, ForeignKey("data_sources.id"), nullable=False)
    model_id = Column(Integer, ForeignKey("models.id"), nullable=False)
    region_id = Column(Integer, ForeignKey("regions.id"), nullable=False)
    datetime_utc = Column(DateTime, nullable=False)
    run_id = Column(String(255), nullable=False)
    last_update = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    __table_args__ = (
        UniqueConstraint("region_id", "data_source_id", "model_id", "datetime_utc", name="forecasts_index"),
    )

    _tracks = relationship("Track", order_by="Track.id", back_populates="_forecast", cascade="all, delete-orphan")
    _model = relationship("Model", back_populates="_forecasts")

    @classmethod
    def from_dict(cls, d):
        return cls(
            id=val.ensure_int_none(d.get("id"), "id"),
            data_source_id=val.ensure_int(d.get("data_source_id"), "data_source_id"),
            model_id=val.ensure_int(d.get("model_id"), "model_id"),
            region_id=val.ensure_int(d.get("region_id"), "region_id"),
            datetime_utc=val.ensure_datetime(d.get("datetime_utc"), "datetime_utc"),
            run_id=val.ensure_str(d.get("run_id", ""), "run_id"),
        )

    @property
    def model_long(self):
        return self._model.long_name

    @property
    def model_short(self):
        return self._model.short_name

    @property
    def data_source(self):
        return self._data_source.long_name

    @property
    def num_tracks(self):
        return len(self._tracks)
