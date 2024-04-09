try:
    from osgeo import ogr
    from osgeo import osr
    from osgeo import gdal
except:
    import ogr
    import osr
    import gdal
from pathlib import Path
import json

from sample_point_creation.distribute_points import create_training_point

import subprocess
import datetime
import geojson

import geopandas as gpd
import pandas as pd
from typing import List

def get_spatial_extent_wgs(file_path):
    """
    get bbox coordinates in WGS84
    """

    site_ds = ogr.Open(file_path)
    site_lyr = site_ds.GetLayer()
    site_feat = site_lyr.GetNextFeature()
    site_geom = site_feat.GetGeometryRef()

    srs_orig = site_lyr.GetSpatialRef()
    srs_dst = osr.SpatialReference()
    srs_dst.ImportFromEPSG(4326)
    transform_wgs = osr.CoordinateTransformation(srs_orig, srs_dst)
    site_geom.Transform(transform_wgs)

    lon, lat = list(), list()
    for i in range(0, site_geom.GetGeometryCount()):
        linestring = site_geom.GetGeometryRef(i)
        for i in range(0, linestring.GetPointCount()):
            point = linestring.GetPoint(i)
            lat.append(point[0])
            lon.append(point[1])

    return {"west": min(lon), "south": min(lat), "east": max(lon), "north": max(lat)}

def max_ndvi_selection(ndvi):
    max_ndvi = ndvi.max()
    return ndvi.array_apply(lambda x:x!=max_ndvi)


def create_points_training_gpkg(training_polygons_filepath, training_column, epsg_value):


    training_polygons_4326_filepath = training_polygons_filepath.parent.joinpath(f"{Path(training_polygons_filepath).stem}_{epsg_value}.gpkg")
    if not training_polygons_4326_filepath.exists():
        cmd = ["ogr2ogr", "-t_srs", f"EPSG: {epsg_value}", "-f", "GPKG",
               str(training_polygons_4326_filepath),
               str(training_polygons_filepath) ]
        subprocess.run(cmd)

    training_points_filepath = training_polygons_filepath.parent.joinpath(f"{Path(training_polygons_filepath).stem}_training_points.gpkg")
    if not training_points_filepath.exists():
        create_training_point(training_polygons_4326_filepath, training_points_filepath, training_column)

    return training_points_filepath


def create_job_dataframe(split_jobs: List[gpd.GeoDataFrame], YEAR: int) -> pd.DataFrame:
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

def generate_output_path(
    root_folder: Path,
    geometry_index: int,
    row: pd.Series
) -> Path:
    features = geojson.loads(row.geometry)
    h3index = features[geometry_index].properties['h3index']
    src_id = features[geometry_index].properties['UID']
    result = root_folder / f"{row.out_prefix}_{h3index}_{src_id}_{geometry_index}{row.out_extension}"
    print("output_path:", result)
    return result

def aggregate_csv(final_csv_path, base_output_path, timestr, id_column, target_column,final_band_names):
    tracking_file = base_output_path / f"tracking_{timestr}.csv"

    tracker_df = pd.read_csv(tracking_file)
    df = pd.DataFrame(columns = final_band_names + ['geometry'])

    for index, row in tracker_df.iterrows():
        if row.status == "finished":
            try:
                # Get the target and geometry from the input
                geometry = gpd.read_file(row.geometry)
                geometry['id'] = geometry['id'].astype(int)
                h3index = geometry.iloc[0]['h3index']
                file_id_name = geometry.iloc[0][id_column]
                filename = f"S1S2-stats_{h3index}_{file_id_name}_0.csv"
                target_df = geometry[["id",id_column, target_column, 'geometry']]

                # Read the stats
                stats_df = pd.read_csv(base_output_path/timestr/filename)
                stats_df.columns = ['id'] + final_band_names

                # Merge the target and geometry with the stats
                stats_df = stats_df.merge(target_df, how='left', on='id')
                stats_df = stats_df.drop(columns=['id'])

                # Append to the dataframe
                df = pd.concat([df, stats_df], ignore_index=True)
            except FileNotFoundError as e:
                print(f"File not found: {filename}")
                pass

    df.to_csv(final_csv_path)



