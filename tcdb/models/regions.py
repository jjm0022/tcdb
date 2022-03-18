from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, TIMESTAMP, text

from tcdb.models.base import Base, DefaultTable
import tcdb.validation as val


class Region(Base, DefaultTable):
    __tablename__ = "regions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    long_name = Column(String(255), nullable=False, unique=True)
    short_name = Column(String(6), nullable=False, unique=True)
    region_char = Column(String(1), nullable=False, unique=True)
    last_update = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    _storms = relationship("Storm", order_by="Storm.id", back_populates="_region")

    def __repr__(self):
        return f"<Region(id={self.id!r}, long_name='{self.long_name!r}', short_name='{self.short_name!r}', region_char='{self.region_char!r}', last_update={self.last_update!r})>"

    @classmethod
    def from_dict(cls, d):
        return cls(
            id=val.ensure_int_none(d.get("id"), "id"),
            long_name=val.ensure_str(d.get("long_name", ""), "long_name"),
            short_name=val.ensure_str(d.get("short_name", ""), "short_name"),
            region_char=val.ensure_str(d.get("region_char", ""), "region_char"),
        )
