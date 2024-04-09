import json

import geopandas as gpd
import pandas as pd
import numpy as np
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

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import numpy as np
from skl2onnx import to_onnx

from helper import aggregate_csv

_log.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
_log.addHandler(stream_handler)

formatter = logging.Formatter('%(asctime)s|%(name)s|%(levelname)s:  %(message)s')
stream_handler.setFormatter(formatter)

from helper import create_points_training_gpkg, create_job_dataframe, generate_output_path
from eodatacube import load_lc_features

# Exclude the other loggers from other libraries
class MyLoggerFilter(logging.Filter):
    def filter(self, record):
        return record.name == _log.name

stream_handler.addFilter(MyLoggerFilter())

def filter_band_names(band_names):
    filtered_band_list = []
    for band_name in band_names:
        if "B" in band_name or "target" in band_name or "NDVI" in band_name or "V" in band_name:
            filtered_band_list.append(band_name)
    return filtered_band_list

########################################################################################################################

## Run these lines to post-process older results
timestr_list = ["20240406-08h47", "20240408-11h06"]
UID = "UID"
target_column = "EUGW_LC"
base_output_path = Path("/home/eouser/userdoc/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data")

def main():

    for timestr in timestr_list:
        final_csv_path = base_output_path.joinpath(f"aggregated_{timestr}.csv")

        json_filepath = base_output_path.joinpath("band_names.json")
        with open(str(json_filepath), 'r') as f:
            final_band_names = json.load(f)

        aggregate_csv(final_csv_path, base_output_path, timestr, UID,target_column ,final_band_names)




if __name__ == "__main__":
    main()