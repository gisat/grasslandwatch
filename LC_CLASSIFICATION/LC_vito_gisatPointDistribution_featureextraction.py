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

def compile_target_UID_dict(target_to_uid, df, target, UID):

    # Loop through each row and populate the dictionary
    for index, row in df.iterrows():
        # Get the EUGW_LC and UID values
        row_target = row[target]
        row_uid = row[UID]

        # If the EUGW_LC is not already a key in the dictionary, add it with the UID as the first item in a list
        if row_target not in target_to_uid:
            target_to_uid[row_target] = [row_uid]
        else:
            # If the EUGW_LC is already a key, append the UID to its list
            target_to_uid[row_target].append(row_uid)

    return target_to_uid


stream_handler.addFilter(MyLoggerFilter())

########################################################################################################################
resource_folder = Path("/home/eouser/userdoc/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/resource")
year = 2020
batch_size = 50
number_sample_eachclass = 150
sitecode = ["CZ0314123_LC_REF"]
training_column = "EUGW_LC"

base_output_path = Path("/home/eouser/userdoc/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data")
base_output_path.mkdir(exist_ok=True)
########################################################################################################################
past_runs = ["20240406-08h47", "20240408-11h06"]
UID = "UID"

uid_list = []
target_to_uid = {}
if past_runs is not None:
    for past_run_item in past_runs:
        features_filepath = base_output_path.joinpath(f"aggregated_{past_run_item}.csv")
        feature_csv = pd.read_csv(features_filepath)
        feature_csv[UID] = feature_csv[UID].astype(int)
        feature_csv[training_column] = feature_csv[training_column].astype(int)

        nan_indices = feature_csv[feature_csv.isnull().any(axis=1)].index
        df_cleaned = feature_csv.drop(nan_indices)

        target_to_uid = compile_target_UID_dict(target_to_uid, df_cleaned[[training_column, UID]], training_column, UID)
        uid_list.extend(feature_csv[UID])
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

if len(uid_list) >0:
    input_gpkg = input_gpkg.loc[~input_gpkg[UID].isin(uid_list)]

# New DataFrame to store the selected rows
new_df = pd.DataFrame()

# Iterate through each unique value in "EU_GW"
for value in input_gpkg['EUGW_LC'].unique():
    # Filter rows for current unique value
    filtered_rows = input_gpkg[input_gpkg['EUGW_LC'] == value]
    # Randomly sample 20 rows from the filtered rows, or take all if less than 20
    if float(value) in target_to_uid.keys():
        number_of_row_needed = number_sample_eachclass - len(target_to_uid[float(value)])
    else:
        number_of_row_needed = number_sample_eachclass

    if number_of_row_needed < 0:
        number_of_row_needed = 0

    sampled_rows = filtered_rows.sample(n=min(number_of_row_needed, len(filtered_rows)), random_state=1) # random_state for reproducibility
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