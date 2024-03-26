

import subprocess

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, ConfusionMatrixDisplay

from sample_point_creation.distribute_points import create_training_point

from eodatacube import create_eodatacube, ndvi_eodatacube, openeo_eodatacube


import openeo
from openeo.extra.spectral_indices.spectral_indices import compute_and_rescale_indices
from openeo.processes import if_, is_nodata, array_concat, array_create
import geopandas as gpd
import pandas as pd
import json
from pathlib import Path
import datetime
from shapely.geometry import box, Point

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, ConfusionMatrixDisplay


def create_square_around_point(point, size=10):
    # Calculate half the size to offset the square around the point
    half_size = size / 2
    # Create a square polygon around the point
    return box(point.x - half_size, point.y - half_size,
               point.x + half_size, point.y + half_size)

print(openeo.__version__)
def main():


    # creation of training polygons
    training_polygons_filepath = Path("/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/CZ_N2K2018.gpkg")
    training_column = "CODE_1_18"
    create_tif = False
    create_table = False
    execute_training_prediction = True

    output_dir = training_polygons_filepath.parent

    start_date = datetime.date(2018, 1, 1)
    end_date = datetime.date(2018, 5, 31)
    time_interval = 'month'
    buffer_size = 0.0005

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

    # c = openeo.connect("openeo.cloud")
    try:
        c.authenticate_oidc()
    except:
        c.authenticate_oidc_device()

    ###### Extract point #######
    # Convert GeoDataFrame to GeoJSON format
    first_item = y_train

    # Create a new GeoDataFrame from the first item, explicitly setting the geometry column
    new_gdf = gpd.GeoDataFrame(first_item, geometry='geometry', crs=training_gdf.crs)
    training_extraction_box_filepath = training_polygons_filepath.parent.joinpath("training_extraction_boxes_4326.gpkg")
    new_gdf.to_file(str(training_extraction_box_filepath), driver="GPKG")
    new_gdf_csv = training_polygons_filepath.parent.joinpath(training_polygons_filepath.name.replace("gpkg", "csv"))
    new_gdf.to_csv(new_gdf_csv)

    ####### eodatacube #######
    # Assuming gdf is your GeoDataFrame
    total_bounds = new_gdf.total_bounds
    # Convert to spatial_extent dictionary
    spatial_extent = {
        "west": total_bounds[0] - buffer_size,
        "south": total_bounds[1]- buffer_size,
        "east": total_bounds[2] + buffer_size,
        "north": total_bounds[3] + buffer_size, "crs": "epsg:4326"}
    eodatacube = openeo_eodatacube(c, spatial_extent, start_date.isoformat(), end_date.isoformat(), time_interval, create_tif, output_dir)


    # Use filter_spatial with the point geometry
    filtered_data = eodatacube.aggregate_spatial(geometries=json.loads(first_item.to_json()), reducer="mean")
    #filtered_data = sentinel2.aggregate_spatial(geometries=geometry_geojson, reducer="mean")



    if create_table:


        # Replace the execute_batch call to use NetCDF format
        job = filtered_data.execute_batch(
            outputfile="result.nc",  # Specify .nc extension for NetCDF
            title="Sentinel composite for Point",
            filename_prefix="point_analysis"
        )

        # Download the results
        job.get_results().download_files(output_dir)

    if execute_training_prediction:
        ml_model = filtered_data.fit_class_random_forest(target=json.loads(y_train.to_json()), num_trees=200)
        model = ml_model.save_ml_model()

        training_job = model.create_job()
        training_job.start_and_wait()


        ###### validation #######
        validation_path = training_polygons_filepath.parent.joinpath("validation")
        validation_path.mkdir(parents=True, exist_ok=True)

        y_test.to_file(filename=str(validation_path.joinpath('y_test.geojson')), driver="GeoJSON")
        cube = filtered_data
        predicted = cube.predict_random_forest(model=training_job, dimension="bands").linear_scale_range(0, 255, 0,
                                                                                                         255).aggregate_spatial(
            json.loads(y_test.to_json()),
            reducer="mean")  # "https://github.com/openEOPlatform/sample-notebooks/raw/main/resources/landcover/model_item.json"
        test_job = predicted.execute_batch(out_format="CSV")
        test_job.get_results().download_files(str(validation_path))

        validation_timeseries_csv = validation_path.joinpath("timeseries.csv")
        df = pd.read_csv(str(validation_timeseries_csv))
        df.index = df.feature_index
        df = df.sort_index()
        df.columns = ["feature_index","predicted"]

        validation_ytest_geojson = validation_path.joinpath('y_test.geojson')
        gdf = gpd.read_file(validation_ytest_geojson)
        gdf['predicted'] = df.predicted.astype(int)

        ConfusionMatrixDisplay.from_predictions(gdf["target"],gdf["predicted"])
        print("--- Accuracy ---")
        print(accuracy_score(gdf["target"],gdf["predicted"]))


if __name__ == "__main__":
    main()
