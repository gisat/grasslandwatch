import inspect

import openeo
from openeo import processes
import xarray
import numpy as np
import io
import requests
import ogr
import osr
import os
import time
import pathlib
import json

import supportive.helper as helper
import pyproj
import matplotlib.pyplot as plt
import matplotlib

# INPUT PARAMETERS: START
# collections = ["LANDSAT8_L2"]
# collections = ["LANDSAT8_L2", "SENTINEL2_L2A"]
collections = ["SENTINEL2_L2A"]
period = ["2017-12-01", "2017-12-31"]
aoi_file = "/media/jiri/ImageArchive/GW_MLTC_TEST/COP4N2K_composite_examples/F_Grande_Brennes/vector/F_AOI.shp"
output_dir = "/media/jiri/ImageArchive/GW_MLTC_TEST/COP4N2K_composite_examples/F_Grande_Brennes/working"
required_bands_sorted = {"LANDSAT8_L2": ["ls_blue", "ls_green", "ls_red", "ls_nir08", "ls_swir16", "ls_swir22"],
                         "SENTINEL2_L2A": ["s2_blue", "s2_green", "s2_red", "s2_re01", "s2_re02", "s2_re03", "s2_nir08", "s2_swir16", "s2_swir22"]}
# INPUT PARAMETERS: END


def run(collections, period, aoi_file, required_bands_sorted):
    """
    A prototype tool for calculating optical composites using the OpenEO platform.
    """

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

    # establish connection to OpenEO backend
    c = (openeo.connect("openeo.dataspace.copernicus.eu"))
    try:
        c.authenticate_oidc()
    except:
        c.authenticate_oidc_device()

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
            collection_id = "SENTINEL2_L2A",
            temporal_extent = period,
            bands = s2_band_codes,
            max_cloud_cover=100)
        s2_bands = s2_bands.filter_bbox(spatial_extent).resample_cube_spatial(target=valid_pixels_mask_dilated, method="near")

        # mask Sentinel-2 bands with the derived invalid pixels mask
        s2_bands = s2_bands.mask(valid_pixels_mask_dilated)

        # rename band labels to the standardized convention
        s2_bands = s2_bands.rename_labels(dimension = "bands", target = required_bands_sorted["SENTINEL2_L2A"], source = s2_band_codes)
        # s2_bands = s2_bands.rename_labels(dimension = "bands", target = list(range(len(s2_band_codes))), source = s2_band_codes)

        # calculate NDVI layer
        ndvi_s2 = s2_bands.ndvi(nir="s2_nir08",red="s2_red")

        rank_mask_s2 = ndvi_s2.apply_neighborhood(
            helper.max_ndvi_selection,
            size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1},
                  {'dimension': 't', 'value': "month"}],
            overlap=[])

        composite_s2 = s2_bands.mask(rank_mask_s2).aggregate_temporal_period("month","first")

        job = composite_s2.execute_batch(out_format="GTiff")
        job.get_results().download_files(output_dir)
        print("Sentinel-2 composite done")
        return

    # For Landsat-8/9 (if required):
    if "LANDSAT8_L2" in collections:

        # load BQA layers as datacube
        bqa = c.load_collection(
            collection_id="LANDSAT8_L2",
            temporal_extent=period,
            bands=["BQA"],
            max_cloud_cover=100)
        bqa = bqa.filter_bbox(spatial_extent)

        # if "SENTINEL2_L2A" in collections:
        #     bqa = bqa.resample_cube_spatial(target=valid_pixels_mask_dilated, method="near")

        # create original valid pixels mask
        valid_pixels_mask_orig = bqa.apply(process=reclassify_ls)

        # apply sieve filter
        valid_pixels_mask_sieved = valid_pixels_mask_orig.apply(process=sieve_ls)

        # dilate invalid pixels area
        valid_pixels_mask_dilated = valid_pixels_mask_sieved.apply(process=dilate_invalids)

        # get list of required Sentinel-2 band codes
        ls_band_codes = [bands_collections_available["LANDSAT8_L2"][band_name] for band_name in
                         required_bands_sorted["LANDSAT8_L2"]]

        # load considered Landsat bands as datacube
        ls_bands = c.load_collection(
            collection_id = "LANDSAT8_L2",
            temporal_extent = period,
            bands = ls_band_codes,
            max_cloud_cover=100)
        ls_bands = ls_bands.filter_bbox(spatial_extent)

        # mask Landsat bands with the derived invalid pixels mask
        ls_bands = ls_bands.mask(valid_pixels_mask_dilated)

        # rename band labels to the standardized convention
        ls_bands = ls_bands.rename_labels(dimension = "bands", target = required_bands_sorted["LANDSAT8_L2"], source = ls_band_codes)
        # ls_bands = ls_bands.rename_labels(dimension = "bands", target = list(range(len(ls_band_codes))), source = ls_band_codes)

        # calculate NDVI layer
        ndvi_ls = ls_bands.ndvi(nir="ls_nir08",red="ls_red")

        rank_mask_ls = ndvi_ls.apply_neighborhood(
            helper.max_ndvi_selection,
            size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1},
                  {'dimension': 't', 'value': "month"}],
            overlap=[])

        # composite_ls = ls_bands.mask(rank_mask_ls).aggregate_temporal_period("month","first")
        composite_ls = ls_bands.mask(rank_mask_ls).aggregate_temporal([
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
        return

start = time.time()

run(collections, period, aoi_file, required_bands_sorted)

stop = time.time()
seconds = stop - start
seconds_remainder = int(seconds % 60)
minutes = seconds/60
minutes_remainder = int(minutes % 60)
hours = int(minutes/60)
print('Processing time: {:02d}:{:02d}:{:02d} - {} [s]'.format(hours, minutes_remainder, seconds_remainder, seconds))
