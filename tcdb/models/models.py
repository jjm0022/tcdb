from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, TIMESTAMP, text

# from tcdb.models import Forecast
from tcdb.models.base import Base, DefaultTable
import tcdb.validation as val


class Model(Base, DefaultTable):
    __tablename__ = "models"

    id = Column(Integer, primary_key=True, autoincrement=True)
    long_name = Column(String(255), nullable=False, unique=True)
    short_name = Column(String(6), nullable=False, unique=True)
    last_update = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    _forecasts = relationship("Forecast", order_by="Forecast.datetime_utc", back_populates="_model", cascade="all, delete-orphan")

    @classmethod
    def from_dict(cls, d):
        return cls(
            id=val.ensure_int_none(d.get("id"), "id"),
            long_name=val.ensure_str(d.get("long_name", ""), "long_name"),
            short_name=val.ensure_str(d.get("short_name", ""), "short_name"),
        )
