"""Settings."""
import logging as lg

# core
# STATIONS_ID_NAME = "station_id"
TIME_NAME = "time"
SJOIN_PREDICATE = "within"

# utils
REQUEST_KWS = {}
PAUSE = 1
ERROR_PAUSE = 60
TIMEOUT = 180
## cache
USE_CACHE = True
CACHE_FOLDER = "./cache"

## logging
LOG_CONSOLE = False
LOG_FILE = False
LOG_FILENAME = "meteostations"
LOG_LEVEL = lg.INFO
LOG_NAME = "meteostations"
LOGS_FOLDER = "./logs"
