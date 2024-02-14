

import subprocess

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, ConfusionMatrixDisplay

from sample_point_creation.distribute_points import create_training_point

from eodatacube import create_eodatacube


import openeo
from openeo.extra.spectral_indices.spectral_indices import compute_and_rescale_indices
from openeo.processes import if_, is_nodata, array_concat, array_create
import geopandas as gpd
import pandas as pd
import json
from pathlib import Path
import datetime
from shapely.geometry import box, Point

def create_square_around_point(point, size=10):
    # Calculate half the size to offset the square around the point
    half_size = size / 2
    # Create a square polygon around the point
    return box(point.x - half_size, point.y - half_size,
               point.x + half_size, point.y + half_size)


def main():


    # creation of training polygons
    training_polygons_filepath = Path("/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/CZ_N2K2018.gpkg")
    training_column = "CODE_1_18"
    create_tif = False

    output_dir = training_polygons_filepath.parent

    start_date = datetime.date(2018, 1, 1)
    end_date = datetime.date(2018, 1, 31)


    training_points_filepath = training_polygons_filepath.parent.joinpath("training_points2.gpkg")
    training_box_filepath = training_polygons_filepath.parent.joinpath("training_points2_box.gpkg")

    if not training_points_filepath.exists():
        create_training_point(training_polygons_filepath, training_points_filepath, training_column)

    if not training_box_filepath.exists():
        gdf = gpd.read_file(training_points_filepath)
        gdf['geometry'] = gdf['geometry'].apply(create_square_around_point)
        gdf.to_file(str(training_box_filepath), driver="GPKG")

    # projec to 4326
    training_boxes_projected_filepath = training_polygons_filepath.parent.joinpath("training_boxes_4326.gpkg")
    if not training_boxes_projected_filepath.exists():
        cmd = ["ogr2ogr", "-t_srs", "EPSG: 4326", "-f", "GPKG",
               str(training_boxes_projected_filepath),
               str(training_box_filepath) ]
        subprocess.run(cmd)

    training_gdf = gpd.read_file(training_boxes_projected_filepath)
    print(training_gdf)

    y_train, y_test = train_test_split(training_gdf, test_size=0.25, random_state=333)
    y_train = y_train[['target','geometry']]
    y_test = y_test[['target','geometry']]

    ####### openeo connection #######
    c = openeo.connect("openeo.dataspace.copernicus.eu")
    try:
        c.authenticate_oidc()
    except:
        c.authenticate_oidc_device()

    ####### eodatacube #######
    # Assuming gdf is your GeoDataFrame
    total_bounds = training_gdf.total_bounds
    # Convert to spatial_extent dictionary
    spatial_extent = {
        "west": total_bounds[0],
        "south": total_bounds[1],
        "east": total_bounds[2],
        "north": total_bounds[3]}
    eodatacube = create_eodatacube(c, spatial_extent, start_date.isoformat(), end_date.isoformat(), create_tif, output_dir)

    ###### Extract point #######
    # Convert GeoDataFrame to GeoJSON format
    first_item = y_train.iloc[0:5]

    # Create a new GeoDataFrame from the first item, explicitly setting the geometry column
    new_gdf = gpd.GeoDataFrame(first_item, geometry='geometry', crs=training_gdf.crs)
    training_extraction_box_filepath = training_polygons_filepath.parent.joinpath("training_extraction_boxes_4326.gpkg")
    new_gdf.to_file(str(training_extraction_box_filepath), driver="GPKG")

    # Use filter_spatial with the point geometry
    filtered_data = eodatacube.aggregate_spatial(json.loads(new_gdf.to_json()), reducer="mean")
    #filtered_data = sentinel2.aggregate_spatial(geometries=geometry_geojson, reducer="mean")

    # Define output format options
    output_format = "NetCDF"  # This may need adjustment based on backend specifics
    output_params = {
        "format": output_format
    }

    # Replace the execute_batch call to use NetCDF format
    job = filtered_data.execute_batch(
        outputfile="result.nc",  # Specify .nc extension for NetCDF
        title="Sentinel composite for Point",
        output_format=output_params,
        filename_prefix="point_analysis"
    )

    # Download the results
    job.get_results().download_files(output_dir)



if __name__ == "__main__":
    main()
