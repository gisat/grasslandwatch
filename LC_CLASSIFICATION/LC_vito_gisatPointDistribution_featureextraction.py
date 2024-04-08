import openeo

import geopandas as gpd
import pandas as pd
import geojson
from pathlib import Path
import datetime
from typing import List
import logging
from pathlib import Path
import json
import numpy as np

from openeo_gfmap.manager import _log
from openeo_gfmap import TemporalContext, Backend, BackendContext, FetchType
from openeo_gfmap.manager.job_splitters import split_job_hex
from openeo_gfmap.manager.job_manager import GFMAPJobManager
from openeo_gfmap.manager import _log
from openeo_gfmap.backend import cdse_connection, vito_connection
from openeo_gfmap.fetching import build_sentinel2_l2a_extractor, build_sentinel1_grd_extractor

global final_band_names
# Global variable to store the final band names
final_band_names = None

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

########################################################################################################################
resource_folder = Path("/home/eouser/userdoc/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/resource")
year = 2020
batch_size = 50
sitecode = ["CZ0314123_LC_REF"]
training_column = "EUGW_LC"

base_output_path = Path("/home/eouser/userdoc/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data")
base_output_path.mkdir(exist_ok=True)

########################################################################################################################
## Training points ##
########################################################################################################################
input_gpkg = gpd.GeoDataFrame()
for file in resource_folder.glob("*.gpkg"):

    if Path(file).stem in sitecode:
        input_gpkg = file
        print("Digesting", file)

training_points_gpkg_filepath = create_points_training_gpkg(input_gpkg, training_column, 3035)
print(f"Done creating {str(training_points_gpkg_filepath)}")
########################################################################################################################
input_gpkg = gpd.read_file(training_points_gpkg_filepath)
# New DataFrame to store the selected rows
new_df = pd.DataFrame()

# Iterate through each unique value in "EU_GW"
for value in input_gpkg['EUGW_LC'].unique():
    # Filter rows for current unique value
    filtered_rows = input_gpkg[input_gpkg['EUGW_LC'] == value]
    # Randomly sample 20 rows from the filtered rows, or take all if less than 20
    sampled_rows = filtered_rows.sample(n=min(50, len(filtered_rows)), random_state=1) # random_state for reproducibility
    # Append these rows to the new DataFrame
    new_df = pd.concat([new_df, sampled_rows], ignore_index=True)
input_df = new_df

#get unique values and counts of each value
unique, counts = np.unique(input_df['EUGW_LC'], return_counts=True)
print(f"unique {unique}, count {counts}")
########################################################################################################################
input_split = split_job_hex(input_df, max_points=batch_size, grid_resolution=4)
print(f"Split the gdf into batchs of {batch_size}.")

job_df = create_job_dataframe(input_split, year)
#job_df = job_df.head(1)
########################################################################################################################

timenow = datetime.datetime.now()
timestr = timenow.strftime("%Y%m%d-%Hh%M")
print(f"Timestr: {timestr}")
tracking_file = base_output_path / f"tracking_{timestr}.csv"

manager = GFMAPJobManager(
    output_dir=base_output_path / timestr,
    output_path_generator=generate_output_path,
    poll_sleep=60,
    n_threads=2,
    collection_id="LC_feature_extraction",
    )

manager.add_backend(Backend.CDSE, cdse_connection, parallel_jobs=2)

manager.run_jobs(
    job_df,
    load_lc_features,
    tracking_file
)

# bands_name_json = base_output_path.joinpath("band_names.json")
#
# # Writing JSON data
# with open(str(bands_name_json), 'w') as f:
#     json.dump(final_band_names, f)