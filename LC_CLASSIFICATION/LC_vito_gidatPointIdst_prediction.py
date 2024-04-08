import openeo
from features import preprocess_features

from rasterio.plot import show
import rasterio
import ogr, osr
import subprocess
from pathlib import Path

resource_folder = Path("/home/eouser/userdoc/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/resource")
sitecode = "CZ0314123"
epsg_value = "4326"


siteextent_gpkg_path = resource_folder.joinpath(f"{sitecode}_EXTENT.gpkg")
if not siteextent_gpkg_path.exists(): raise Exception()

siteextent_gpkg_4326_filepath = siteextent_gpkg_path.parent.joinpath(f"{Path(siteextent_gpkg_path).stem}_{epsg_value}.gpkg")
if not siteextent_gpkg_4326_filepath.exists():
    cmd = ["ogr2ogr", "-t_srs", f"EPSG: {epsg_value}", "-f", "GPKG",
           str(siteextent_gpkg_4326_filepath),
           str(siteextent_gpkg_path) ]
    subprocess.run(cmd)


processing_tiles_ds = ogr.Open(str(siteextent_gpkg_4326_filepath))
processing_tiles_lyr = processing_tiles_ds.GetLayer()
processing_tiles_srs = processing_tiles_lyr.GetSpatialRef()
processing_tiles_epsg = processing_tiles_srs.GetAttrValue('AUTHORITY', 1)

for feature in processing_tiles_lyr:
    # Get the geometry of the current feature
    geom = feature.GetGeometryRef()

    # Get the extent of the geometry
    extent = geom.GetEnvelope()  # Returns a tuple (minX, maxX, minY, maxY)

    # Extract the west, east, south, and north coordinates from the extent
    west, east, south, north = extent

    # Use the coordinates as needed, for example, print them
    print(f"Feature ID: {feature.GetFID()}, West: {west}, East: {east}, South: {south}, North: {north}")

# Test parameters
south =  48.91851466
north = 48.94012633
west = 14.26612782
east = 14.30958627
bbox = {"west": round(west, 2), "south": round(south, 2), "east": round(east,  2), "north": round(north,  2)}
#14.26612782,48.91851466,14.30958627,48.94012633
#bbox = {"west": 5.0, "south": 51.2, "east": 5.015, "north": 51.215}

YEAR_FROM = 2015
YEAR_TO = 2020

for year_item in range(YEAR_FROM, YEAR_TO + 1):
    temporal_extent = (f"{year_item}-01-01", f"{year_item}-12-31")

    print(f"temporal extent: {temporal_extent}, bbox: {bbox}")

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
    model_url = "https://s3.waw3-1.cloudferro.com/swift/v1/models/random_forest_20240406-08h47.onnx"
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


    result_path = f"output/predictions/prediction_LC_{sitecode}_{temporal_extent[0]}_{temporal_extent[1]}_{Path(model_url).stem}_prediction.tiff"
    prediction_job.download_result(result_path)

# with rasterio.open(result_path) as src:
#     show(src, title="Prediction")