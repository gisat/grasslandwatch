import openeo

from MTC_PROTOTYPING.test_logging_from_udf.opt_mtc import get_spatial_extent_wgs

output_dir = "/home/yantra/gisat/src/grasslandwatch/output"
aoi_file = "/home/yantra/gisat/src/grasslandwatch/MTC_PROTOTYPING/test_logging_from_udf/test_aoi/AT3304000_BBox.shp"

c = openeo.connect("openeo.cloud")
c.authenticate_oidc()

period = ["2022-06-01", "2022-06-30"]

# get AOI spatial extent
spatial_extent = get_spatial_extent_wgs(aoi_file)

sentinel2 = c.load_collection(
    "SENTINEL2_L2A",
    temporal_extent = period,
    bands = ["B02", "B03", "B04","SCL"],
    max_cloud_cover=95
)

sentinel2 = sentinel2.process(
            "mask_scl_dilation",
            data=sentinel2,
            scl_band_name="SCL",
            kernel1_size=17, kernel2_size=77,
            mask1_values=[2, 4, 5, 6, 7],
            mask2_values=[3, 8, 9, 10, 11],
            erosion_kernel_size=3)

sentinel2 = sentinel2.aggregate_temporal_period("month",reducer="median")\
    .apply_dimension(dimension="t", process="array_interpolate_linear")


S1_collection = "SENTINEL1_GRD"
if "SENTINEL1_GRD_SIGMA0" in c.list_collection_ids():
    S1_collection = "SENTINEL1_GRD_SIGMA0"

sentinel1 = c.load_collection(
    S1_collection,
    temporal_extent = period,
    bands = ["VV","VH"]
)

if S1_collection == "SENTINEL1_GRD":
    sentinel1 = sentinel1.sar_backscatter(
        coefficient='sigma0-ellipsoid',
        local_incidence_angle=False,
        elevation_model='COPERNICUS_30')

sentinel1 = sentinel1.aggregate_temporal_period("month",reducer="median")\
    .apply_dimension(dimension="t", process="array_interpolate_linear")

merged = sentinel2.merge_cubes(sentinel1)

my_udf = openeo.UDF("""
from openeo.udf import XarrayDataCube
from openeo.udf.debug import inspect

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    array = cube.get_array()
    inspect(array,level="ERROR",message="inspecting input cube")
    array.values = 0.0001 * array.values
    return cube
""")

fused = merged.apply_neighborhood(my_udf, size=[
        {'dimension': 'x', 'value': 112, 'unit': 'px'},
        {'dimension': 'y', 'value': 112, 'unit': 'px'}
    ], overlap=[
        {'dimension': 'x', 'value': 8, 'unit': 'px'},
        {'dimension': 'y', 'value': 8, 'unit': 'px'}
    ])


spatial_extent = {'west': 4.45, 'east': 4.48, 'south': 51.16, 'north': 51.19, 'crs': 'epsg:4326'}

job = fused.filter_bbox(spatial_extent).execute_batch("result.tif", title="Sentinel composite", filename_prefix="merged_cube")
job.get_results().download_files(output_dir)
