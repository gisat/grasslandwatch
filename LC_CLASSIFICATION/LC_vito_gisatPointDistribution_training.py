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
timestr = "20240406-08h47"
UID = "UID"
target_column = "EUGW_LC"
base_output_path = Path("/home/eouser/userdoc/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data")

def main():
    final_csv_path = base_output_path.joinpath(f"aggregated_{timestr}.csv")

    json_filepath = base_output_path.joinpath("band_names.json")
    with open(str(json_filepath), 'r') as f:
        final_band_names = json.load(f)

    if not final_csv_path.exists():
        aggregate_csv(final_csv_path, base_output_path, timestr, UID,target_column ,final_band_names)
        df = pd.read_csv(final_csv_path)
    else:
        df = pd.read_csv(final_csv_path)

    ################################################
    band_names = filter_band_names(final_band_names)
    ################################################

    X = df[band_names]
    X = X.astype(np.float32)  # convert to float32 to allow ONNX conversion later on
    y = df[target_column].astype(int)

    # Step 1: Find indices of rows with NaN in df1
    nan_indices = X[X.isnull().any(axis=1)].index
    # Step 2: Drop these rows from both DataFrames
    X_cleaned = X.drop(nan_indices)
    y_corresponding = y.drop(nan_indices)
    X_train, X_test, y_train, y_test = train_test_split(X_cleaned, y_corresponding, test_size=0.3, random_state=42)
    unique, counts = np.unique(y_train, return_counts=True)
    print(f"unique {unique}, count {counts}")

    rf = RandomForestClassifier(n_estimators=100, max_features=y.unique().size, random_state=42)
    rf = rf.fit(X_train, y_train)

    y_pred = rf.predict(X_test)
    print("Accuracy on test set: " + str(accuracy_score(y_test, y_pred))[0:5])

    model_output_path = base_output_path / "models"
    model_output_path.mkdir(exist_ok=True)

    onnx = to_onnx(model=rf, name="random_forest", X=X_train.values)

    with open(base_output_path / "models" / f"random_forest_{timestr}.onnx", "wb") as f:
        f.write(onnx.SerializeToString())




if __name__ == "__main__":
    main()