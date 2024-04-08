import openeo
from features import preprocess_features

from rasterio.plot import show
import rasterio

# Test parameters
bbox = {"west": 5.0, "south": 51.2, "east": 5.05, "north": 51.25}
temporal_extent = ("2020-01-01", "2020-12-31")

# Setup the connection
connection = openeo.connect("openeo.dataspace.copernicus.eu")
connection.authenticate_oidc()

# Load S1 and S2 collections
s1 = connection.load_collection(
    "SENTINEL1_GRD",
    spatial_extent= bbox,
    temporal_extent=temporal_extent,
    bands=["VH", "VV"],
).sar_backscatter( # GFMap performed this step in the extraction
    coefficient="sigma0-ellipsoid"
)
s2 = connection.load_collection(
    "SENTINEL2_L2A",
    spatial_extent= bbox,
    temporal_extent=temporal_extent,
    bands=["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B11", "B12", "SCL"],
    max_cloud_cover=80
)

# Preprocess the features
features = preprocess_features(s2_datacube=s2, s1_datacube=s1)

# Supply the model as a URL and create an UDF from a file
model_url = "https://s3.waw3-1.cloudferro.com/swift/v1/models/random_forest_20240405-22h35.onnx"
udf = openeo.UDF.from_file(
    "udf_rf_onnx.py",
    context={
        "model_url": model_url
    }
)

# Reduce the bands dimesnion to a single prediction using the udf
prediction = features.reduce_bands(reducer=udf)

# Add the onnx dependencies to the job options. You can reuse this existing dependencies archive
dependencies_url = "https://artifactory.vgt.vito.be/artifactory/auxdata-public/openeo/onnx_dependencies_1.16.3.zip"
job_options = {
    "udf-dependency-archives": [
        f"{dependencies_url}#onnx_deps"
    ],
}

prediction_job = prediction.create_job(
    out_format="GTiff",
    job_options=job_options,
    title="LC_prediction"
)
prediction_job.start_and_wait()

result_path = "output/predictions/prediction.tiff"
prediction_job.download_result(result_path)
