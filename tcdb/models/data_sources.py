from sqlalchemy import Column, Integer, String, TIMESTAMP, text

from tcdb.models.base import Base, DefaultTable
import tcdb.validation as val


class DataSource(Base, DefaultTable):
    __tablename__ = "data_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    long_name = Column(String(255), nullable=False, unique=True)
    short_name = Column(String(10), nullable=False, unique=True)
    last_update = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    @classmethod
    def from_dict(cls, d):
        return cls(
            id=val.ensure_int_none(d.get("id"), "id"),
            long_name=val.ensure_str(d.get("long_name", ""), "long_name"),
            short_name=val.ensure_str(d.get("short_name", ""), "short_name"),
        )
