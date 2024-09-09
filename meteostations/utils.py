"""Utils.

Based on osmnx utils and downloader modules.
"""

import datetime as dt
import json
import logging as lg
import os
import sys
import unicodedata
from contextlib import redirect_stdout
from hashlib import sha1
from pathlib import Path
from typing import Union

import pandas as pd

from meteostations import settings


def dms_to_decimal(ser: pd.Series) -> pd.Series:
    """Convert a series from degrees, minutes, seconds (DMS) to decimal degrees."""
    degrees = ser.str[0:2].astype(int)
    minutes = ser.str[3:4].astype(int)
    seconds = ser.str[5:6].astype(int)
    direction = ser.str[-1]

    decimal = degrees + minutes / 60 + seconds / 3600
    decimal = decimal.where(direction.isin(["N", "E"]), -decimal)

    return decimal


def ts(*, style: str = "datetime", template: Union[str, None] = None) -> str:
    """Get current timestamp as string.

    Parameters
    ----------
    style : str {"datetime", "date", "time"}
        Format the timestamp with this built-in template.
    template : str
        If not None, format the timestamp with this template instead of one of the
        built-in styles.

    Returns
    -------
    ts : str
        The string timestamp.

    """
    if template is None:
        if style == "datetime":
            template = "{:%Y-%m-%d %H:%M:%S}"
        elif style == "date":
            template = "{:%Y-%m-%d}"
        elif style == "time":
            template = "{:%H:%M:%S}"
        else:  # pragma: no cover
            raise ValueError(f"unrecognized timestamp style {style!r}")

    ts = template.format(dt.datetime.now())
    return ts


def _get_logger(level: int, name: str, filename: str) -> lg.Logger:
    """Create a logger or return the current one if already instantiated.

    Parameters
    ----------
    level : int
        One of Python's logger.level constants.
    name : string
        Name of the logger.
    filename : string
        Name of the log file, without file extension.

    Returns
    -------
    logger : logging.logger

    """
    logger = lg.getLogger(name)

    # if a logger with this name is not already set up
    if not getattr(logger, "handler_set", None):
        # get today's date and construct a log filename
        log_filename = Path(settings.LOGS_FOLDER) / f'{filename}_{ts(style="date")}.log'

        # if the logs folder does not already exist, create it
        log_filename.parent.mkdir(parents=True, exist_ok=True)

        # create file handler and log formatter and set them up
        handler = lg.FileHandler(log_filename, encoding="utf-8")
        formatter = lg.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.handler_set = True

    return logger


def log(
    message: str,
    *,
    level: Union[int, None] = None,
    name: Union[str, None] = None,
    filename: Union[str, None] = None,
) -> None:
    """Write a message to the logger.

    This logs to file and/or prints to the console (terminal), depending on the current
    configuration of settings.LOG_FILE and settings.LOG_CONSOLE.

    Parameters
    ----------
    message : str
        The message to log.
    level : int
        One of Python's logger.level constants.
    name : str
        Name of the logger.
    filename : str
        Name of the log file, without file extension.

    """
    if level is None:
        level = settings.LOG_LEVEL
    if name is None:
        name = settings.LOG_NAME
    if filename is None:
        filename = settings.LOG_FILENAME

    # if logging to file is turned on
    if settings.LOG_FILE:
        # get the current logger (or create a new one, if none), then log message at
        # requested level
        logger = _get_logger(level=level, name=name, filename=filename)
        if level == lg.DEBUG:
            logger.debug(message)
        elif level == lg.INFO:
            logger.info(message)
        elif level == lg.WARNING:
            logger.warning(message)
        elif level == lg.ERROR:
            logger.error(message)

    # if logging to console (terminal window) is turned on
    if settings.LOG_CONSOLE:
        # prepend timestamp
        message = f"{ts()} {message}"

        # convert to ascii so it doesn't break windows terminals
        message = (
            unicodedata.normalize("NFKD", str(message))
            .encode("ascii", errors="replace")
            .decode()
        )

        # print explicitly to terminal in case jupyter notebook is the stdout
        if getattr(sys.stdout, "_original_stdstream_copy", None) is not None:
            # redirect captured pipe back to original
            os.dup2(sys.stdout._original_stdstream_copy, sys.__stdout__.fileno())
            sys.stdout._original_stdstream_copy = None
        with redirect_stdout(sys.__stdout__):
            print(message, file=sys.__stdout__, flush=True)


def _url_in_cache(url: str) -> Union[Path, None]:
    """Determine if a URL's response exists in the cache.

    Calculates the checksum of url to determine the cache file's name.

    Parameters
    ----------
    url : str
        URL to look for in the cache.

    Returns
    -------
    filepath : pathlib.Path
        Path to cached response for url if it exists, otherwise None.

    """
    # hash the url to generate the cache filename
    filename = sha1(url.encode("utf-8")).hexdigest() + ".json"
    filepath = Path(settings.CACHE_FOLDER) / filename

    # if this file exists in the cache, return its full path
    return filepath if filepath.is_file() else None


def _retrieve_from_cache(url: str, *, check_remark: bool = False) -> Union[dict, None]:
    """Retrieve a HTTP response JSON object from the cache, if it exists.

    Parameters
    ----------
    url : str
        URL of the request.
    check_remark : bool, default False
        If True, only return filepath if cached response does not have a remark key
        indicating a server warning.

    Returns
    -------
    response_json : dict
        Cached response for the url if it exists in the cache, otherwise None.

    """
    # if the tool is configured to use the cache
    if settings.USE_CACHE:
        # return cached response for this url if exists, otherwise return None
        cache_filepath = _url_in_cache(url)
        if cache_filepath is not None:
            response_json = json.loads(cache_filepath.read_text(encoding="utf-8"))

            # return None if check_remark is True and there is a server remark in the
            # cached response
            if check_remark and "remark" in response_json:
                log(f"Found remark, so ignoring cache file {cache_filepath!r}")
                return None

            log(f"Retrieved response from cache file {cache_filepath!r}")
            return response_json


def _save_to_cache(url: str, response_json: dict, sc: int) -> None:
    """Save a HTTP response JSON object to a file in the cache folder.

    Function calculates the checksum of url to generate the cache file's name. If the
    request was sent to server via POST instead of GET, then URL should be a GET-style
    representation of request. Response is only saved to a cache file if
    settings.USE_CACHE is True, response_json is not None, and sc = 200.

    Users should always pass OrderedDicts instead of dicts of parameters into request
    functions, so the parameters remain in the same order each time, producing the same
    URL string, and thus the same hash. Otherwise the cache will eventually contain
    multiple saved responses for the same request because the URL's parameters appeared
    in a different order each time.

    Parameters
    ----------
    url : str
        URL of the request.
    response_json : dict
        JSON response.
    sc : int
        response's HTTP status code.

    """
    if settings.USE_CACHE:
        if sc != 200:
            log(f"Did not save to cache because status code is {sc}")

        elif response_json is None:
            log("Did not save to cache because response_json is None")

        else:
            # create the folder on the disk if it doesn't already exist
            cache_folder = Path(settings.CACHE_FOLDER)
            cache_folder.mkdir(parents=True, exist_ok=True)

            # hash the url to make the filename succinct but unique
            # sha1 digest is 160 bits = 20 bytes = 40 hexadecimal characters
            filename = sha1(url.encode("utf-8")).hexdigest() + ".json"
            cache_filepath = cache_folder / filename

            # dump to json, and save to file
            cache_filepath.write_text(json.dumps(response_json), encoding="utf-8")
            log(f"Saved response to cache file {cache_filepath!r}")
