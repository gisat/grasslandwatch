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

from openeo import processes
import xarray
import numpy as np
import io
import requests
import os
import time
import pathlib
import json

import supportive.helper as helper
import pyproj
import matplotlib.pyplot as plt
import matplotlib

collections = ["SENTINEL2_L2A"]
period = ["2017-12-01", "2017-12-31"]
aoi_file = "/home/yantra/gisat/src/grasslandwatch/MTC_PROTOTYPING/test_logging_from_udf/test_aoi/AT3304000_BBox.shp"
output_dir = "/home/yantra/gisat/src/grasslandwatch/output"
required_bands_sorted = {"LANDSAT8_L2": ["ls_blue", "ls_green", "ls_red", "ls_nir08", "ls_swir16", "ls_swir22"],
                         "SENTINEL2_L2A": ["s2_blue", "s2_green", "s2_red", "s2_re01", "s2_re02", "s2_re03", "s2_nir08", "s2_swir16", "s2_swir22"]}
# INPUT PARAMETERS: END

# establish connection to OpenEO backend
c = (openeo.connect("openeo.dataspace.copernicus.eu"))
try:
    c.authenticate_oidc()
except:
    c.authenticate_oidc_device()

from MTC_PROTOTYPING.OPT_MTC_NDVI_MAX.opt_mtc import run as eodata_opt_run
start = time.time()

# get a base directory
base_dir = pathlib.Path(__file__).parent.resolve()

# load band codes in collections
with open(base_dir.joinpath("supportive", "bands.json")) as bdo:
    bands_collections_available = json.load(bdo)

# TODO: zjistit, zda se da UDF parametrizovat, implementovat resampling Landsatu

#  load UDFs
reclassify_s2 = openeo.UDF.from_file(base_dir.joinpath("udf", "reclassify_s2.py"))
reclassify_ls = openeo.UDF.from_file(base_dir.joinpath("udf", "reclassify_ls.py"))
dilate_invalids = openeo.UDF.from_file(base_dir.joinpath("udf", "dilate_invalids.py"))
sieve_s2 = openeo.UDF.from_file(base_dir.joinpath("udf", "sieve_s2.py"))
sieve_ls = openeo.UDF.from_file(base_dir.joinpath("udf", "sieve_ls.py"))

# get AOI spatial extent
spatial_extent = helper.get_spatial_extent_wgs(aoi_file)

# For Sentinel-2 (if required):
if "SENTINEL2_L2A" in collections:
    # load SCL layers as datacube
    scl = c.load_collection(
        collection_id="SENTINEL2_L2A",
        temporal_extent=period,
        bands=["SCL"],
        max_cloud_cover=100)
    scl = scl.filter_bbox(spatial_extent).resample_spatial(resolution=20, method="near")

    # create original valid pixels mask
    valid_pixels_mask_orig = scl.apply(process=reclassify_s2)

    # apply sieve filter
    valid_pixels_mask_sieved = valid_pixels_mask_orig.apply(process=sieve_s2)

    # dilate invalid pixels area
    valid_pixels_mask_dilated = valid_pixels_mask_sieved.apply(process=dilate_invalids)

    # get list of required Sentinel-2 band codes
    s2_band_codes = [bands_collections_available["SENTINEL2_L2A"][band_name] for band_name in
                     required_bands_sorted["SENTINEL2_L2A"]]

    # load considered Sentinel-2 bands as datacube
    s2_bands = c.load_collection(
        collection_id="SENTINEL2_L2A",
        temporal_extent=period,
        bands=s2_band_codes,
        max_cloud_cover=100)
    s2_bands = s2_bands.filter_bbox(spatial_extent).resample_cube_spatial(target=valid_pixels_mask_dilated,
                                                                          method="near")

    # mask Sentinel-2 bands with the derived invalid pixels mask
    s2_bands = s2_bands.mask(valid_pixels_mask_dilated)

    # rename band labels to the standardized convention
    s2_bands = s2_bands.rename_labels(dimension="bands", target=required_bands_sorted["SENTINEL2_L2A"],
                                      source=s2_band_codes)
    # s2_bands = s2_bands.rename_labels(dimension = "bands", target = list(range(len(s2_band_codes))), source = s2_band_codes)

    # calculate NDVI layer
    ndvi_s2 = s2_bands.ndvi(nir="s2_nir08", red="s2_red")

    rank_mask_s2 = ndvi_s2.apply_neighborhood(
        helper.max_ndvi_selection,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1},
              {'dimension': 't', 'value': "month"}],
        overlap=[])

    composite_s2 = s2_bands.mask(rank_mask_s2).aggregate_temporal_period("month", "first")

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

merged = composite_s2.merge_cubes(sentinel1)

job = merged.execute_batch(out_format="GTiff")
job.get_results().download_files(output_dir)
print("Sentinel-2 composite done")

stop = time.time()
seconds = stop - start
seconds_remainder = int(seconds % 60)
minutes = seconds/60
minutes_remainder = int(minutes % 60)
hours = int(minutes/60)
print('Processing time: {:02d}:{:02d}:{:02d} - {} [s]'.format(hours, minutes_remainder, seconds_remainder, seconds))
