from loguru import logger
import numpy as np
import datetime


def validate_pressure(x, raise_on_fail=False):
    #TODO: Need to work out a way to deal with mslp == None rather than just using 1000mb
    if x is None:
        return 1000
    if (x >= 850) and (x <= 1050):
        return x
    else:
        if raise_on_fail:
            logger.error(f"Unrealistic value of {x} identified for pressure")
            raise ValueError(f"Unrealistic value of {x} identified for pressure")
        else:
            logger.trace(f"Unrealistic value of {x} identified for pressure")
            return None


def validate_velocity(x, raise_on_fail=False):
    if (x >= 0) and (x <= 250):
        return x
    else:
        if raise_on_fail:
            logger.error(f"Unrealistic value of {x} identified for latitude")
            raise ValueError(f"Unrealistic value of {x} identified for latitude")
        else:
            logger.trace(f"Unrealistic value of {x} identified for velocity")
            return None


def validate_latitude(x, raise_on_fail=False):
    if (x >= -90) and (x <= 90):
        return x
    else:
        if raise_on_fail:
            logger.error(f"Unrealistic value of {x} identified for latitude")
            raise ValueError(f"Unrealistic value of {x} identified for latitude")
        else:
            logger.trace(f"Unrealistic value of {x} identified for latitude")
            return None


def validate_longitude(x, raise_on_fail=False):
    if (x >= -180) and (x <= 180):
        return x
    else:
        if raise_on_fail:
            logger.error(f"Unrealistic value of {x} identified for longitude")
            raise ValueError(f"Unrealistic value of {x} identified for longitude")
        else:
            logger.trace(f"Unrealistic value of {x} identified for longitude")
            return None


def validate_distance(x):
    if x is None:
        return None
    if x >= 0:
        return x
    else:
        logger.trace(f"Unrealistic value of {x} identified for distance")
        return None


def validate_direction(x):
    if x is None:
        return None
    if (x >= 0) and (x <= 360):
        return x
    else:
        logger.trace(f"Unrealistic value of {x} identified for direction")
        return None


def validate_forecast_step(x, raise_on_fail=False):
    if x >= 0:
        return x
    else:
        if raise_on_fail:
            logger.error(f"Unrealistic value of {x} identified for forecast step")
            raise ValueError(f"Unrealistic value of {x} identified for forecast step")
        else:
            logger.trace(f"Unrealistic value of {x} identified for forecast step")
            return None


def ensure_int_none(x, key=""):
    if isinstance(x, int) or x is None:
        return x
    else:
        if isinstance(x, np.int64):
            return int(x)
        else:
            raise ValueError(f"Expected '{key}' to be `int` or `None` not {type(x)} [{x}] type")


def ensure_int(x, key=""):
    if isinstance(x, int):
        return x
    else:
        if isinstance(x, np.int64):
            return int(x)
        else:
            raise ValueError(f"Expected '{key}' to be `int` not {type(x)} [{x}] type")


def ensure_str(x, key=""):
    if isinstance(x, str):
        return x
    else:
        raise ValueError(f"Expected '{key}' to be `str` not {type(x)} [{x}] type")


def ensure_datetime(x, key=""):
    if isinstance(x, datetime.datetime):
        return x
    else:
        raise ValueError(f"Expected '{key}' to be `datetime.datetim` not `{type(x)}` [{x}] type")
