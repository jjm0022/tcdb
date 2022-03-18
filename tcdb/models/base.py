from loguru import logger
import datetime

from sqlalchemy.orm import declarative_base
from tcdb.utils import is_serializable
from tcdb.formatting import pretty_print

Base = declarative_base()


class DefaultTable(object):
    def __repr__(self):
        summary = [f"<{__file__.split('/')[-1].split('.')[0]}.{self.__tablename__}>"]
        col_width = max([len(col.name) for col in self.__table__.columns]) + 3
        for col in self.__table__.columns:
            col_name = pretty_print(col.name, col_width)
            summary.append(f"{' '*4}{col_name}{self.__getattribute__(col.name)!r}")
        return "\n".join(summary) + "\n"

    def to_dict(self, serializable=False):
        """Convert a table class object to a dict

        Args:
            serializable (bool, optional): Convert all object to a type that can be serialized and saved to a JSON file. Defaults to False

        Raises:
            ValueError: Raised for types that I'm not sure what to convert to

        Returns:
            dict
        """
        out_dict = dict()
        for col in self.__table__.columns:
            out_dict[col.name] = self.__getattribute__(col.name)

        if serializable:
            for key, value in out_dict.items():
                if is_serializable(value):
                    continue
                else:
                    if isinstance(value, datetime.datetime):
                        logger.trace(f"Converting {key} from {type(value)} to str")
                        out_dict[key] = value.isoformat()
                    else:
                        logger.error(f"Not sure how to make {key} ({type(value)}) serializable")
                        raise ValueError
        return out_dict
