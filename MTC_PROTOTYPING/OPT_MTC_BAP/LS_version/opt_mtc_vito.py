import openeo
import xarray
import numpy as np
import io
import requests
import pathlib
import json
from openeo.processes import if_, is_nan

# import panel as pn

import pyproj
import matplotlib.pyplot as plt
import matplotlib

import supportive.helper as helper

period = ["2015-04-01", "2015-06-30"]
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

# load BQA layers as datacube
bqa = c.load_collection(
    collection_id="LANDSAT8_L2",
    temporal_extent=period,
    bands=["BQA"],
    max_cloud_cover=95)
bqa = bqa.filter_bbox(spatial_extent)

bqa = bqa.apply(lambda x: if_(is_nan(x), 0, x))

score = bqa.apply_neighborhood(
    process=openeo.UDF.from_file("udf_score.py"),
    size=[{'dimension': 'x', 'unit': 'px', 'value': 256}, {'dimension': 'y', 'unit': 'px', 'value': 256}],
    overlap=[{'dimension': 'x', 'unit': 'px', 'value': 16}, {'dimension': 'y', 'unit': 'px', 'value': 16}]
)

score = score.rename_labels('bands', ['score'])

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
ls_band_codes = [bands_collections_available["LANDSAT8_L2"][band_name] for band_name in
                 required_bands_sorted["LANDSAT8_L2"]]

ls_bands = c.load_collection(
    "LANDSAT8_L2",
    temporal_extent = period,
    spatial_extent = spatial_extent,
    bands = ls_band_codes,
    max_cloud_cover=95
)

# composite_ls = ls_bands.mask(rank_mask_ls).aggregate_temporal_period("month","first")
composite_ls = ls_bands.mask(rank_mask).aggregate_temporal([
    ["2015-01-01", "2015-03-31"],
    ["2015-04-01", "2015-06-30"],
    ["2015-07-01", "2015-09-30"],
    ["2015-10-01", "2015-12-31"],
    ["2018-01-01", "2018-03-31"],
    ["2018-04-01", "2018-06-30"],
    ["2018-07-01", "2018-09-30"],
    ["2018-10-01", "2018-12-31"]],
    "first")

job = composite_ls.execute_batch(out_format="GTiff")
job.get_results().download_files(output_dir)
print("Landsat composite done")