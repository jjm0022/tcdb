from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy import Column, Integer, Float, String, DateTime, TIMESTAMP, text

from tcdb.models.base import Base, DefaultTable
import tcdb.validation as val


class Observation(Base, DefaultTable):
    __tablename__ = "observations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    storm_id = Column(Integer, ForeignKey("storms.id"), nullable=False)
    datetime_utc = Column(DateTime, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    intensity_kts = Column(Float, nullable=False)
    mslp_mb = Column(Float, nullable=False)
    r34_ne = Column(Integer)
    r34_se = Column(Integer)
    r34_sw = Column(Integer)
    r34_nw = Column(Integer)
    r50_ne = Column(Integer)
    r50_se = Column(Integer)
    r50_sw = Column(Integer)
    r50_nw = Column(Integer)
    r64_ne = Column(Integer)
    r64_se = Column(Integer)
    r64_sw = Column(Integer)
    r64_nw = Column(Integer)
    pouter_mb = Column(Integer)
    router_nmi = Column(Integer)
    rmw_nmi = Column(Integer)
    run_id = Column(String(255), nullable=False)
    last_update = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    __table_args__ = (UniqueConstraint("storm_id", "datetime_utc", name="observations_index"),)

    _storm = relationship("Storm", back_populates="_observations")

    @classmethod
    def from_dict(cls, d):
        return cls(
            id=val.ensure_int_none(d.get("id"), "id"),
            storm_id=val.ensure_int(d.get("storm_id"), "storm_id"),
            datetime_utc=val.ensure_datetime(d.get("datetime_utc"), "datetime_utc"),
            latitude=val.validate_latitude(d.get("latitude"), raise_on_fail=True),
            longitude=val.validate_longitude(d.get("longitude"), raise_on_fail=True),
            intensity_kts=val.validate_velocity(d.get("intensity_kts"), raise_on_fail=True),
            mslp_mb=val.validate_pressure(d.get("mslp_mb")),
            r34_ne=val.validate_distance(d.get("r34_ne")),
            r34_se=val.validate_distance(d.get("r34_se")),
            r34_sw=val.validate_distance(d.get("r34_sw")),
            r34_nw=val.validate_distance(d.get("r34_nw")),
            r50_ne=val.validate_distance(d.get("r50_ne")),
            r50_se=val.validate_distance(d.get("r50_se")),
            r50_sw=val.validate_distance(d.get("r50_sw")),
            r50_nw=val.validate_distance(d.get("r50_nw")),
            r64_ne=val.validate_distance(d.get("r64_ne")),
            r64_se=val.validate_distance(d.get("r64_se")),
            r64_sw=val.validate_distance(d.get("r64_sw")),
            r64_nw=val.validate_distance(d.get("r64_nw")),
            pouter_mb=val.validate_pressure(d.get("pouter_mb")),
            router_nmi=val.validate_distance(d.get("router_nmi")),
            rmw_nmi=val.validate_distance(d.get("rmw_nmi")),
            run_id=val.ensure_str(d.get("run_id", ""), "run_id"),
        )

    @property
    def storm_name(self):
        return self._storm.name
