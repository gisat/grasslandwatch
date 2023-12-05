import openeo
import xarray
import numpy as np
import io
import requests
import pathlib
import json

# import panel as pn

import pyproj
import matplotlib.pyplot as plt
import matplotlib

import supportive.helper as helper

period = ["2020-09-05", "2020-09-30"]
aoi_file = "/media/jiri/ImageArchive/GW_MLTC_TEST/COP4N2K_composite_examples/HU_Hortobagy/vector/HU_AOI.shp"
output_dir = "/media/jiri/ImageArchive/GW_MLTC_TEST/COP4N2K_composite_examples/HU_Hortobagy/working"
required_bands_sorted = {"LANDSAT8_L2": ["ls_blue", "ls_green", "ls_red", "ls_nir08", "ls_swir16", "ls_swir22"],
                         "SENTINEL2_L2A": ["s2_blue", "s2_green", "s2_red", "s2_re01", "s2_re02", "s2_re03", "s2_nir08", "s2_swir16", "s2_swir22"]}

# establish connection to OpenEO backend
c = (openeo.connect("openeo.dataspace.copernicus.eu"))
try:
    c.authenticate_oidc()
except:
    c.authenticate_oidc_device()

# get AOI spatial extent
spatial_extent = helper.get_spatial_extent_wgs(aoi_file)

scl = c.load_collection(
    "SENTINEL2_L2A",
    spatial_extent=spatial_extent,
    temporal_extent=period,
    bands=["SCL"],
    max_cloud_cover=(95) # VITO had 70
)

score = scl.apply_neighborhood(
    process=openeo.UDF.from_file("udf_score.py"),
    size=[{'dimension': 'x', 'unit': 'px', 'value': 256}, {'dimension': 'y', 'unit': 'px', 'value': 256}],
    overlap=[]
)

def max_score_selection(score):
    max_score = score.max()
    return score.array_apply(lambda x:x!=max_score)

rank_mask = score.apply_neighborhood(
        max_score_selection,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1},
              {'dimension': 't', 'value': "month"}],
        overlap=[]
    )

# get a base directory
base_dir = pathlib.Path(__file__).parent.resolve()
# load band codes in collections
with open(base_dir.joinpath("supportive", "bands.json")) as bdo:
    bands_collections_available = json.load(bdo)

# get list of required Sentinel-2 band codes
s2_band_codes = [bands_collections_available["SENTINEL2_L2A"][band_name] for band_name in
                 required_bands_sorted["SENTINEL2_L2A"]]

s2_bands = c.load_collection(
    "SENTINEL2_L2A",
    temporal_extent = period,
    spatial_extent = spatial_extent,
    bands = s2_band_codes,
    max_cloud_cover=95
)

composite = s2_bands.mask(rank_mask).aggregate_temporal_period("month","first")

job = composite.execute_batch(out_format="GTiff")
job.get_results().download_files(output_dir)
print("Landsat composite done")