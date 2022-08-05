from turtle import up
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

    def updateFromDict(self, updates, check_only=False):
        """Compare the values in `updates` to the values of attributes with the same name in 
        the current instance.

        If differences are found, set the attribute to the updated value

        All keys in `updates` must exist as attributes in the instance.
        Will only compare values that are given in `updates`. This means that not all attributes in 
            self are required to  be in `updates`. This is helpful for instances where a quick check
            of lat, lon, wind values are needed but none of the other attributes need to be checked.
        If `check_only` is True, values in the instance are not modified.

        Args:
            updates (dict): Dictionary containg values to compare to the attribute of the current instance
            check_only (bool): If True, only compares the values, does not modify the instance attributes. Default False

        Returns:
            list[str]: list of attributes that were updated 
        """
        updated_keys = list()
        for key, value in updates.items():
            if self.__getattribute__(key) != value:
                if not check_only:
                    # always log changes to storm records
                    if self.__tablename__  in ['storms', 'steps']:
                        logger.info(f"Updating {self.__tablename__}.{key} for record {self.id} from {self.__getattribute__(key)} to {value}")
                    else:
                        logger.debug(f"Updating {self.__tablename__}.{key} for record {self.id} from {self.__getattribute__(key)} to {value}")
                    self.__setattr__(key, value)
                updated_keys.append(key)
        return updated_keys 