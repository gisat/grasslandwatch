import os

import openeo
from openeo.extra.spectral_indices.spectral_indices import compute_and_rescale_indices
from openeo.processes import if_, is_nodata, array_concat, array_create
import geopandas as gpd
import pandas as pd
import json
from pathlib import Path
import datetime
from shapely.geometry import box

from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, ConfusionMatrixDisplay


validation_path = Path("/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/openeo_val")
os.makedirs(validation_path, exist_ok=True)

mask = box(4.4, 50.2, 5.6, 51.2)
y = gpd.read_file("https://artifactory.vgt.vito.be/auxdata-public/openeo/LUCAS_2018_Copernicus.gpkg",mask=mask)
y["geometry"] = y["geometry"].apply(lambda x: x.centroid)
y["LC1"] = y["LC1"].apply(lambda x: ord(x[0])-65)
y_train, y_test = train_test_split(y, test_size=0.25, random_state=333)

y_train["target"] = y_train.LC1
y_train = y_train[['target','geometry']]
y_test["target"] = y_test.LC1
y_test = y_test[['target','geometry']]
y_train


c = openeo.connect("openeo.dataspace.copernicus.eu")
c.authenticate_oidc()


def connection():
    return c


def sentinel2_composite(start_date, end_date, connection_provider, provider, index_dict=None, s2_list=[],
                        processing_opts={}, sampling=False, stepsize=10, overlap=10, reducer="median", luc=False,
                        cloud_procedure="sen2cor"):
    """
    Compute a cloud-masked, gap-filled, Sentinel-2 datacube, composited at 10-daily intervals.
    """
    temp_ext_s2 = [start_date.isoformat(), end_date.isoformat()]

    props = {
        "eo:cloud_cover": lambda v: v <= 80
    }

    bands = ["B03", "B04", "B05", "B06", "B07", "B08", "B11", "B12", "SCL"]

    c = connection_provider()
    s2 = c.load_collection("SENTINEL2_L2A",
                           temporal_extent=temp_ext_s2,
                           bands=bands,
                           properties=props)

    s2 = s2.process("mask_scl_dilation", data=s2, scl_band_name="SCL").filter_bands(s2.metadata.band_names[:-1])

    indices = compute_and_rescale_indices(s2, index_dict, True).filter_bands(
        s2_list + list(index_dict["indices"].keys()))
    idx_dekad = indices.aggregate_temporal_period("dekad", reducer="median")

    idx_dekad = idx_dekad.apply_dimension(dimension="t", process="array_interpolate_linear")
    return idx_dekad


def sentinel1_composite(start_date, end_date, connection_provider=connection, provider="Terrascope", processing_opts={},
                        relativeOrbit=None, orbitDirection=None, sampling=False, stepsize=12, overlap=6,
                        reducer="mean"):
    c = connection_provider()
    temp_ext_s1 = [start_date.isoformat(), end_date.isoformat()]

    s1_id = "SENTINEL1_GRD"
    properties = {}

    if orbitDirection is not None:
        properties["orbitDirection"] = lambda p: p == orbitDirection

    s1 = c.load_collection(s1_id,
                           temporal_extent=temp_ext_s1,
                           bands=["VH", "VV"],
                           properties=properties
                           )

    if (provider.upper() != "TERRASCOPE"):
        s1 = s1.sar_backscatter(coefficient="sigma0-ellipsoid")

    # Observed Ranges:
    # VV: 0 - 0.3 - Db: -20 .. 0
    # VH: 0 - 0.3 - Db: -30 .. -5
    # Ratio: 0- 1
    # S1_GRD = S1_GRD.apply(lambda x: 10 * x.log(base=10))
    s1 = s1.apply_dimension(dimension="bands",
                            process=lambda x: array_create(
                                [30.0 * x[0] / x[1], 30.0 + 10.0 * x[0].log(base=10), 30.0 + 10.0 * x[1].log(base=10)]))
    s1 = s1.rename_labels("bands", ["ratio"] + s1.metadata.band_names)
    # scale to int16
    s1 = s1.linear_scale_range(0, 30, 0, 30000)
    s1_dekad = s1.aggregate_temporal_period(period="dekad", reducer="median")

    s1_dekad = s1_dekad.apply_dimension(dimension="t", process="array_interpolate_linear")
    return s1_dekad


def compute_statistics_fill_nan(base_features, start_date, end_date, stepsize):
    """
    Computes statistics over a datacube.
    For correct statistics, the datacube needs to be preprocessed to contain observation at equitemporal intervals, without nodata values.

    """

    def computeStats(input_timeseries, sample_stepsize, offset):
        tsteps = list([input_timeseries.array_element(offset + sample_stepsize * index) for index in range(0, 6)])
        tsteps[1] = if_(is_nodata(tsteps[1]), tsteps[2], tsteps[1])
        tsteps[4] = if_(is_nodata(tsteps[4]), tsteps[3], tsteps[4])
        tsteps[0] = if_(is_nodata(tsteps[0]), tsteps[1], tsteps[0])
        tsteps[5] = if_(is_nodata(tsteps[5]), tsteps[4], tsteps[5])
        return array_concat(
            array_concat(input_timeseries.quantiles(probabilities=[0.25, 0.5, 0.75]), input_timeseries.sd()), tsteps)

    tot_samples = (end_date - start_date).days // stepsize
    nr_tsteps = 6
    sample_stepsize = tot_samples // nr_tsteps
    offset = int(sample_stepsize / 2 + (tot_samples % nr_tsteps) / 2)

    features = base_features.apply_dimension(dimension='t', target_dimension='bands',
                                             process=lambda x: computeStats(x, sample_stepsize, offset))

    tstep_labels = ["t" + str(offset + sample_stepsize * index) for index in range(0, 6)]
    all_bands = [band + "_" + stat for band in base_features.metadata.band_names for stat in
                 ["p25", "p50", "p75", "sd"] + tstep_labels]
    features = features.rename_labels('bands', all_bands)
    return features


def load_lc_features(provider, feature_raster, start_date, end_date, stepsize_s2=10, stepsize_s1=12, processing_opts={},
                     index_dict=None, connection_provider=connection):
    if not index_dict:
        idx_list = ["NDVI", "NDMI", "NDGI", "NDRE1", "NDRE2", "NDRE5"]
        s2_list = ["B06", "B12"]
        index_dict = {idx: [-1, 1] for idx in idx_list}
        index_dict["ANIR"] = [0, 1]

    final_index_dict = {
        "collection": {
            "input_range": [0, 8000],
            "output_range": [0, 30000]
        },
        "indices": {
            index: {"input_range": index_dict[index], "output_range": [0, 30000]} for index in index_dict
        }
    }

    idx_dekad = sentinel2_composite(start_date, end_date, connection_provider, provider, final_index_dict, s2_list,
                                    processing_opts=processing_opts, sampling=True, stepsize=stepsize_s2, luc=True)
    idx_features = compute_statistics_fill_nan(idx_dekad, start_date, end_date, stepsize=stepsize_s2)

    s1_dekad = sentinel1_composite(start_date, end_date, connection_provider, provider, processing_opts=processing_opts,
                                   orbitDirection="ASCENDING", sampling=True, stepsize=stepsize_s1)
    s1_features = compute_statistics_fill_nan(s1_dekad, start_date, end_date, stepsize=stepsize_s1)

    features = idx_features.merge_cubes(s1_features)

    return features, features.metadata.band_names

features, feature_list = load_lc_features("terrascope", "both", datetime.date(2018, 3, 1), datetime.date(2018, 10, 31))
X = features.aggregate_spatial(json.loads(y_train.to_json()), reducer="mean")
ml_model = X.fit_class_random_forest(target=json.loads(y_train.to_json()), num_trees=200)
model = ml_model.save_ml_model()

training_job = model.create_job()
training_job.start_and_wait()

###### validation #######

y_test.to_file(filename=str(validation_path.joinpath('y_test.geojson')), driver="GeoJSON")
cube = X
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
df.columns = ["feature_index", "predicted"]

validation_ytest_geojson = validation_path.joinpath('y_test.geojson')
gdf = gpd.read_file(validation_ytest_geojson)
gdf['predicted'] = df.predicted.astype(int)

ConfusionMatrixDisplay.from_predictions(gdf["target"], gdf["predicted"])
print("--- Accuracy ---")
print(accuracy_score(gdf["target"], gdf["predicted"]))