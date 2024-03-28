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


resource_folder = Path("/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/resource")
YEAR = 2020
sitecode = ["CZ0314123_LC_REF"]

input_gpkg = gpd.GeoDataFrame()
for file in resource_folder.glob("*.gpkg"):

    if Path(file).stem in sitecode:
        input_gpkg = pd.concat([input_gpkg, gpd.read_file(file)], ignore_index=True, sort=False, copy=False)
        print("Digesting", file)

input_gpkg["geometry"] = input_gpkg["geometry"].apply(lambda x: x.centroid)
input_gpkg = input_gpkg[['UID',"CODE_1_18", "CODE_2_18", "CODE_3_18", "CODE_4_18",'geometry', 'REF_TYPE']]

input_split = split_job_hex(input_gpkg, max_points=50, grid_resolution=4)

def create_job_dataframe(split_jobs: List[gpd.GeoDataFrame]) -> pd.DataFrame:
    """Create a dataframe from the split jobs, containg all the necessary information to run the job."""
    rows = []
    for job in split_jobs:
        start_date = datetime.datetime(YEAR, 1, 1)
        end_date = datetime.datetime(YEAR, 12, 31)
        rows.append(pd.Series({
            'out_prefix': 'S1S2-stats',
            'out_extension': '.csv',
            'start_date': start_date,
            'end_date': end_date,
            'geometry': job.to_json()
        }))
    return pd.DataFrame(rows)

job_df = create_job_dataframe(input_split)

job_df = job_df.head(1) # testing: only run one job for now

from lc_offline.features import preprocess_features


def sentinel2_collection(
        row: pd.Series,
        connection: openeo.DataCube,
        geometry: geojson.FeatureCollection
) -> openeo.DataCube:
    bands = ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B11", "B12", "SCL"]
    bands_with_platform = ["S2-L2A-" + band for band in bands]

    extraction_parameters = {
        "load_collection": {
            "eo:cloud_cover": lambda val: val <= 80.0,
        },
    }

    extractor = build_sentinel2_l2a_extractor(
        backend_context=BackendContext(Backend(row.backend_name)),
        bands=bands_with_platform,
        fetch_type=FetchType.POINT,
        **extraction_parameters
    )

    temporal_context = TemporalContext(row.start_date, row.end_date)

    s2 = extractor.get_cube(connection, geometry, temporal_context)
    s2 = s2.rename_labels("bands", bands)
    return s2


def sentinel1_collection(
        row: pd.Series,
        connection: openeo.DataCube,
        geometry: geojson.FeatureCollection,
) -> openeo.DataCube:
    bands = ["VH", "VV"]
    bands_with_platform = ["S1-SIGMA0-" + band for band in bands]

    extractor = build_sentinel1_grd_extractor(
        backend_context=BackendContext(Backend(row.backend_name)),
        bands=bands_with_platform,
        fetch_type=FetchType.POINT,
    )

    temporal_context = TemporalContext(row.start_date, row.end_date)

    s1 = extractor.get_cube(connection, geometry, temporal_context)
    s1 = s1.rename_labels("bands", bands)
    return s1


def load_lc_features(
        row: pd.Series,
        connection: openeo.DataCube,
        **kwargs
):
    geometry = geojson.loads(row.geometry)

    s2_collection = sentinel2_collection(
        row=row,
        connection=connection,
        geometry=geometry
    )

    s1_collection = sentinel1_collection(
        row=row,
        connection=connection,
        geometry=geometry
    )

    features = preprocess_features(s2_collection, s1_collection)

    # Currently, aggregate_spatial and vectorcubes do not keep the band names, so we'll need to rename them later on
    global final_band_names
    final_band_names = [b.name for b in features.metadata.band_dimension.bands]

    features = features.aggregate_spatial(geometry, reducer="median")

    job_options = {
        "executor-memory": "3G",  # Increase this value if a job fails due to memory issues
        "executor-memoryOverhead": "2G",
        "soft-errors": True
    }

    return features.create_job(
        out_format="csv",
        title=f"GFMAP_Extraction_{geometry.features[0].properties['h3index']}",
        job_options=job_options,
    )


# Global variable to store the final band names
final_band_names = None

def generate_output_path(
    root_folder: Path,
    geometry_index: int,
    row: pd.Series
) -> Path:
    features = geojson.loads(row.geometry)
    h3index = features[geometry_index].properties['h3index']
    result = root_folder / f"{row.out_prefix}_{h3index}_{geometry_index}{row.out_extension}"
    print("output_path:", result)
    return result

base_output_path = Path("/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data")
base_output_path.mkdir(exist_ok=True)

timenow = datetime.datetime.now()
timestr = timenow.strftime("%Y%m%d-%Hh%M")
print(f"Timestr: {timestr}")
tracking_file = base_output_path / f"tracking_{timestr}.csv"

manager = GFMAPJobManager(
    output_dir=base_output_path / timestr,
    output_path_generator=generate_output_path,
    poll_sleep=60,
    n_threads=1,
    collection_id="LC_feature_extraction",
)

manager.add_backend(Backend.CDSE, cdse_connection, parallel_jobs=2)

manager.run_jobs(
    job_df,
    load_lc_features,
    tracking_file
)

## Run these lines to post-process older results
timestr = "20240318-20h30"
tracking_file = base_output_path / f"tracking_{timestr}.csv"

tracker_df = pd.read_csv(tracking_file)
df = pd.DataFrame(columns = final_band_names + ["CODE_1_18", "CODE_2_18", "CODE_3_18", "CODE_4_18", 'geometry'])

for index, row in tracker_df.iterrows():
    if row.status == "finished":
        try:
            # Get the target and geometry from the input
            geometry = gpd.read_file(row.geometry)
            geometry['id'] = geometry['id'].astype(int)
            h3index = geometry.iloc[0]['h3index']
            filename = f"S1S2-stats_{h3index}_0.csv"
            target_df = geometry[['id', "CODE_1_18", "CODE_2_18", "CODE_3_18", "CODE_4_18", 'geometry']]

            # Read the stats
            stats_df = pd.read_csv(base_output_path/timestr/filename)
            stats_df.columns = ['id'] + final_band_names

            # Merge the target and geometry with the stats
            stats_df = stats_df.merge(target_df, how='left', on='id')
            stats_df = stats_df.drop(columns=['id'])

            # Append to the dataframe
            df = pd.concat([df, stats_df])
        except FileNotFoundError as e:
            print(f"File not found: {filename}")
            pass

df.to_csv(base_output_path / timestr / "features.csv", index=False)

