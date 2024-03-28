import openeo

import geopandas as gpd
import pandas as pd
import geojson
from pathlib import Path
import datetime
from typing import List
import logging
from pathlib import Path

from openeo_gfmap.manager import _log
from openeo_gfmap import TemporalContext, Backend, BackendContext, FetchType
from openeo_gfmap.manager.job_splitters import split_job_hex
from openeo_gfmap.manager.job_manager import GFMAPJobManager
from openeo_gfmap.manager import _log
from openeo_gfmap.backend import cdse_connection, vito_connection
from openeo_gfmap.fetching import build_sentinel2_l2a_extractor, build_sentinel1_grd_extractor

_log.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
_log.addHandler(stream_handler)

formatter = logging.Formatter('%(asctime)s|%(name)s|%(levelname)s:  %(message)s')
stream_handler.setFormatter(formatter)

# Exclude the other loggers from other libraries
class MyLoggerFilter(logging.Filter):
    def filter(self, record):
        return record.name == _log.name

stream_handler.addFilter(MyLoggerFilter())
########################################################################################################################

resource_folder = Path("/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/resource")
YEAR = 2020
sitecode = ["CZ0314123_LC_REF"]


input_gpkg = gpd.GeoDataFrame()
for file in resource_folder.glob("*.gpkg"):

    if Path(file).stem in sitecode:
        input_gpkg = pd.concat([input_gpkg, gpd.read_file(file)], ignore_index=True, sort=False, copy=False)
        print("Digesting", file)

create_points_training_gpkg(input_gpkg)

