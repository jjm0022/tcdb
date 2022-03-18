from scipy.io import loadmat, matlab
import xarray as xr
import pandas as pd
import numpy as np
from loguru import logger
from IPython.core.debugger import set_trace

from dataclasses import dataclass
from datetime import datetime, timedelta
from xarray.core.formatting import format_array_flat, format_timestamp, pretty_print

from collections import Counter
from tcdb.config import settings
from tcdb.utils import greatCircleDistance
import tcdb.validation as val


@dataclass
class Syntrack:
    init: datetime = np.datetime64("NaT")
    storm_name: str = "NaN'"
    annual_id: int = np.nan
    ens_num: int = np.nan
    ens_name: str = "NaN"
    hour: np.ndarray = np.array([np.nan])
    valid: np.ndarray = np.array([np.nan])
    lat: np.ndarray = np.array([np.nan])
    lon: np.ndarray = np.array([np.nan])
    mslp: np.ndarray = np.array([np.nan])
    wind: np.ndarray = np.array([np.nan])
    model: str = "NaN"
    track_type: str = "NaN"

    def __repr__(self) -> str:
        width = 60
        summary = ["\n<cfan.Syntrack>"]
        field_keys = {
            "storm_name": self.storm_name,
            "annual_id": self.annual_id,
            "init": format_timestamp(self.init),
            "ens_num": self.ens_num,
            "ens_name": self.ens_name,
            "hour": f"{self.hour.dtype} {format_array_flat(self.hour, width)}",
            "valid": f"{self.valid.dtype} {format_array_flat(self.valid, width)}",
            "lat": f"{self.lat.dtype} {format_array_flat(self.lat, width)}",
            "lon": f"{self.lon.dtype} {format_array_flat(self.lon, width)}",
            "wind": f"{self.wind.dtype} {format_array_flat(self.wind, width)}",
            "mslp": f"{self.mslp.dtype} {format_array_flat(self.mslp, width)}",
        }
        info_keys = {
            "Empty": self.empty(),
            "Valid Steps": self.validSteps(),
            "Start": f"{format_timestamp(self.valid.min())} ({self.hour.min()})",
            "End": f"{format_timestamp(self.valid.max())} ({self.hour.max()})",
            "Max Intensity": f"{self.wind.max():0.2f}",
            "Min Pressure": f"{self.mslp.min():0.2f}",
            "Model": self.model.upper(),
            "Track Type": self.track_type,
        }

        col_width = np.max([len(key) for key in info_keys.keys()]) + 3
        col_width = max(np.max([len(key) for key in field_keys.keys()]) + 3, col_width)
        # format track fields
        summary.append("Fields:")
        for key in field_keys.keys():
            key_name = pretty_print(key, col_width)
            summary.append(f'{" "*4}{key_name}{field_keys[key]}')

        summary.append("Track Information:")
        # format track information
        for key in info_keys.keys():
            key_name = pretty_print(key, col_width)
            summary.append(f'{" "*4}{key_name}{info_keys[key]}')

        return "\n".join(summary) + "\n"

    def __getitem__(self, key):
        return super().__getattribute__(key)

    def validSteps(self):
        return np.count_nonzero(~np.isnan(self.lat))

    def empty(self):
        # count number of elements that are NOT nan
        if self.validSteps() >= 1:
            return False
        else:
            return True


def to_xarray(tracks, periods=None):

    valid_tracks = [ens_num for ens_num, track in tracks.items() if not track.empty()]
    if len(valid_tracks) == 0:
        logger.warning(f"Cannot convert to xarray because there are no valid tracks")
        return None

    model = most_common(tracks, "model")
    model_settings = settings.get(model)
    model_res = model_settings.get("temporal_resolution", 6)
    num_ens = model_settings.get("num_ens")
    forecast_init = most_common(tracks, "init")

    names = [""] * num_ens
    for t in tracks.values():
        names[t.ens_num - 1] = t.ens_name
    if not periods:
        periods = int(model_settings.get("max_step") / model_res) + 1

    step = range(0, (model_res * periods), model_res)
    valid = pd.date_range(forecast_init, periods=periods, freq=f"{model_res}H")
    hour = np.ones((num_ens, len(valid))) * np.nan
    lat = hour.copy()
    lon = hour.copy()
    wind = hour.copy()
    mslp = hour.copy()

    for ens, track in tracks.items():
        if track.empty():
            continue
        # get the indices where the valid times match up
        inds = np.where((valid >= track.valid.min()) & (valid <= track.valid.max()))
        hour[ens - 1, inds] = track.hour.astype(int)
        lat[ens - 1, inds] = track.lat
        lon[ens - 1, inds] = track.lon
        wind[ens - 1, inds] = track.wind
        mslp[ens - 1, inds] = track.mslp

    # try to get storm name and number. Assume the most common is correct
    storm_name = most_common(tracks, "storm_name")
    annual_id = most_common(tracks, "annual_id")

    ds = xr.Dataset(
        data_vars=dict(
            hour=(["ensemble", "step"], hour),
            lat=(["ensemble", "step"], lat),
            lon=(["ensemble", "step"], lon),
            wind=(["ensemble", "step"], wind),
            mslp=(["ensemble", "step"], mslp),
        ),
        coords=dict(
            ensemble=np.arange(1, num_ens + 1),
            step=step,
            ensemble_name=(["ensemble"], names),
            valid_time=(["step"], valid),
        ),
        attrs=dict(Model=model, ForecastInitDatetime=forecast_init, StormName=storm_name, AnnualId=annual_id),
    )
    return ds


def ensMean(tracks, return_ds=False, median=False, min_ensembles=0.0):
    valid_tracks = [ens_num for ens_num, track in tracks.items() if not track.empty()]
    if len(valid_tracks) == 0:
        logger.warning(f"Cannot convert to xarray because there are no valid tracks")
        return Syntrack(ens_name="ENS_MEAN")

    ds = to_xarray(tracks)
    # determine the model so we can get the correct number of ensembles
    model = most_common(tracks, "model")
    model_settings = settings.get(model)
    num_ens = model_settings.get("num_ens")
    # get all ensembles except deterministic
    d = ds.sel(ensemble=range(1, num_ens - 1))
    if median:
        d = d.median(dim="ensemble")
    else:
        d = d.mean(dim="ensemble")

    if return_ds:
        # Cant combine the ensemble mean back to the original DS until you add ensemble and ensemble_name
        d = d.expand_dims({"ensemble": [num_ens + 1]})
        d = d.assign_coords({"ensemble_name": "ENS_MEAN"})
        return xr.concat([ds, d], dim="ensemble")
    else:
        t = Syntrack(
            storm_name=most_common(tracks, "storm_name"),
            init=most_common(tracks, "init"),
            annual_id=most_common(tracks, "annual_id"),
            ens_num=num_ens + 1,
            ens_name="ENS_MEAN",
            hour=d.hour.values.astype(int),
            valid=d.valid_time.values,
            lat=np.round(d.lat.values, 3),
            lon=np.round(d.lon.values, 3),
            mslp=np.round(d.mslp.values, 3),
            wind=np.round(d.wind.values, 3),
            model=model,
            track_type=most_common(tracks, "track_type"),
        )
        return t


def most_common(tracks, field):
    field_counter = Counter()
    for track in tracks.values():
        if track.empty():
            continue
        field_counter.update([track[field]])
    return field_counter.most_common()[0][0]


def _check_keys(data):
    """
    checks if entries in dictionary are mat-objects. If yes
    todict is called to change them to nested dictionaries
    """
    for key in data:
        if isinstance(data[key], matlab.mio5_params.mat_struct):
            data[key] = _todict(data[key])
    return data


def _todict(matobj):
    """
    A recursive function which constructs from matobjects nested dictionaries
    """
    data_dict = {}
    for strg in matobj._fieldnames:
        elem = matobj.__dict__[strg]
        if isinstance(elem, matlab.mio5_params.mat_struct):
            data_dict[strg] = _todict(elem)
        else:
            data_dict[strg] = elem
    return data_dict


def toSyntrackObjects(struct, init_datetime, model, track_type):
    tracks = list()
    for track in struct:
        if track.ens == -1:
            ens_name = "DET"
            ens_num = settings.get(model).num_ens
        elif track.ens == 0:
            ens_name = "CTRL"
            ens_num = track.ens + 1
        else:
            ens_name = "ENS"
            ens_num = track.ens + 1

        if isinstance(track.stormName, str):
            storm_name = track.stormName
        else:
            storm_name = ""

        if isinstance(track.hour, np.ndarray):
            valid = np.array([init_datetime + timedelta(hours=int(h)) for h in track.hour])
        else:
            valid = init_datetime + timedelta(hours=track.hour)

        t = Syntrack(
            storm_name=storm_name,
            init=init_datetime,
            annual_id=track.annual_id,
            ens_num=ens_num,
            ens_name=ens_name,
            hour=ensureArray(track.hour),
            valid=ensureArray(valid),
            lat=np.round(ensureArray(track.lat), 3),
            lon=np.round(ensureArray(track.lon), 3),
            mslp=np.round(ensureArray(track.mslp), 3),
            wind=np.round(ensureArray(track.wind), 3),
            model=model.upper(),
            track_type=track_type,
        )

        tracks.append(t)

    return tracks


def ensureArray(var):
    if isinstance(var, np.ndarray):
        return var
    elif isinstance(var, (float, int, str, datetime)):
        return np.array([var])
    else:
        raise TypeError(f"Didn't account for this type: {type(var)}")


def parseSystemTracks(tracks, inv, time_threshold=1.5, ens_mean=True):
    """

    Args:
        tracks ([type]): [description]
        inv ([type]): [description]
        time_threshold (float, optional): Maximum number of days from invest.valid_time to consider matching tracks. Useful when
            a tracks first step is something other than 0. Defaults to 1.5.

    Returns:
        [type]: [description]
    """

    model_settings = settings.get(tracks[0].model)

    steps = np.arange(0, model_settings.get("max_step") + 6, 6)
    distance_threshold = np.linspace(310, 1450, len(steps))
    num_ens = model_settings.get("num_ens")

    invest_tracks = {t.ens_num: t for t in tracks if t.annual_id == inv.annual_id}
    # no need to proceed if there's not any tracks for the provided invest
    if len(invest_tracks) == 0:
        logger.warning(f"No tracks found for {inv.name}")
        return {i: Syntrack(ens_num=i) for i in range(1, num_ens + 1)}

    # check to see if every ensemble has a track
    if len(invest_tracks) < num_ens:
        for ens in range(1, num_ens + 1):
            if ens in invest_tracks.keys():  # matched by tracker
                continue
            tmp_tracks = [t for t in tracks if t.ens_num == ens]
            # No need to see if the ensemble track belongs to the storm if there's not any tracks for the ensemble
            if len(tmp_tracks) > 0:
                invest_tracks[ens] = checkUnassignedTracks(tmp_tracks, inv, distance_threshold[0], time_threshold)
            else:
                invest_tracks[ens] = Syntrack(ens_num=ens)
    # put the tracks in order
    final_tracks = dict()
    for ens in range(1, num_ens + 1):
        final_tracks[ens] = invest_tracks.get(ens)
    if ens_mean:
        # add ensemble mean
        final_tracks[num_ens + 1] = ensMean(final_tracks)

    return final_tracks


def checkUnassignedTracks(tracks, inv, dist_threshold, time_threshold):
    """Iterate through a list of Tracks and check to see if any unassigned tracks can be matched with the
    provided invest. Unassigned tracks have an annual_id of 0.

    Args:
        tracks (list[Track]): A list of Track objects
        inv (Invest): An Invest object
        dist_threshold (float/int): Maximum distance (nm) that a track can start from an invest
        time_threshold (float): Maximum number of days from invest.valid_time to consider matching tracks. Useful when
            a track's first step is something other than 0.

    Returns:
        Track: If an unassigned track is matched with the invest
        None: If no unassigned tracks were matched with the invest
    """

    passing_tracks = list()
    for track in tracks:
        # make sure we're not using a track that's already addigned to a different storm
        if track.annual_id != 0:
            continue
        # first step is more than 1.5 days away from invest observation
        if (track.valid[0] - inv.valid) >= timedelta(days=time_threshold):
            continue
        # make sure storm is within distance threshold
        # distance = greatCircleDistance(track.lat[0], track.lon[0], inv.latitude, inv.longitude) can only be used if we're not using the in-house cfan-invest files
        distance = greatCircleDistance(track.lat[0], track.lon[0], inv.lat, inv.lon)
        if distance <= dist_threshold:
            passing_tracks.append((track, distance, len(track.hour)))

    if len(passing_tracks) == 0:
        # final_track = None #NOTE might want to change this so that it returns an "empty" track instead
        ens_num = {t.ens_num for t in tracks}
        ens_name = {t.ens_name for t in tracks}
        final_track = Syntrack(ens_name=list(ens_name)[0], ens_num=list(ens_num)[0])

    elif len(passing_tracks) == 1:
        final_track = passing_tracks[0][0]
    else:
        longest = 0
        for track in passing_tracks:
            if track[2] > longest:
                final_track = track[0]
                longest = track[2]

    final_track.annual_id = inv.annual_id
    final_track.storm_name = inv.name
    return final_track


def toStepDict(syn, step_index, track_id):

    step = dict(
        track_id=track_id,
        hour=val.validate_forecast_step(int(syn.hour[step_index]), raise_on_fail=True),
        latitude=val.validate_latitude(float(syn.lat[step_index]), raise_on_fail=True),
        longitude=val.validate_longitude(float(syn.lon[step_index]), raise_on_fail=True),
        intensity_kts=val.validate_velocity(float(syn.wind[step_index]), raise_on_fail=True),
        mslp_mb=round(val.validate_pressure(float(syn.mslp[step_index])), 2),
    )

    return step
