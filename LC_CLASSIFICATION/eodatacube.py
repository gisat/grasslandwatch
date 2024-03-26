import openeo
import xarray
import numpy as np
import io
import requests
import pathlib
import json
from openeo.processes import if_, is_nan
import os
import time
# import panel as p
import scipy.signal

import pyproj
import matplotlib.pyplot as plt
import matplotlib

start = time.time()

import helper


required_bands_sorted = {"LANDSAT8_L2": ["ls_blue", "ls_green", "ls_red", "ls_nir08", "ls_swir16", "ls_swir22"],
                         "SENTINEL2_L2A": ["s2_blue", "s2_green", "s2_red", "s2_re01", "s2_re02", "s2_re03", "s2_nir08", "s2_swir16", "s2_swir22"]}
spatial_resolution = 20
max_cloud_cover = 90

#  load UDFs
base_dir = pathlib.Path(__file__).parent.resolve()
reclassify_s2 = openeo.UDF.from_file(base_dir.joinpath("udf", "reclassify_s2.py"))
dilate_invalids = openeo.UDF.from_file(base_dir.joinpath("udf", "dilate_invalids.py"))
sieve_s2 = openeo.UDF.from_file(base_dir.joinpath("udf", "sieve_s2.py"))


def create_eodatacube(c, spatial_extent, start_date, end_date, create_opt_tiff, output_period_dir, tif_filename = "composite"):

    period = [start_date, end_date]

    scl = c.load_collection(
        "SENTINEL2_L2A",
        spatial_extent=spatial_extent,
        temporal_extent=period,
        bands=["SCL"],
        max_cloud_cover=max_cloud_cover
    ).resample_spatial(spatial_resolution)

    scl = scl.apply(lambda x: if_(is_nan(x), 0, x))

    score = scl.apply_neighborhood(
        process=openeo.UDF.from_file("udf_score.py"),
        size=[{'dimension': 'x', 'unit': 'px', 'value': 256}, {'dimension': 'y', 'unit': 'px', 'value': 256}],
        overlap=[{'dimension': 'x', 'unit': 'px', 'value': 16}, {'dimension': 'y', 'unit': 'px', 'value': 16}]
    )

    score = score.rename_labels('bands', ['score'])

    def max_score_selection(score):
        max_score = score.max()
        return score.array_apply(lambda x: x != max_score)


    rank_mask = score.apply_neighborhood(
        max_score_selection,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1},
              {'dimension': 't', 'value': "month"}],
        overlap=[]
    )

    rank_mask = rank_mask.band('score')

    # get a base directory
    base_dir = pathlib.Path(__file__).parent.resolve()
    # load band codes in collections
    with open(base_dir.joinpath("bands.json")) as bdo:
        bands_collections_available = json.load(bdo)

    # get list of required Sentinel-2 band codes
    s2_band_codes = [bands_collections_available["SENTINEL2_L2A"][band_name] for band_name in
                     required_bands_sorted["SENTINEL2_L2A"]]

    s2_bands = c.load_collection(
        "SENTINEL2_L2A",
        temporal_extent = period,
        spatial_extent = spatial_extent,
        bands = s2_band_codes,
        max_cloud_cover=max_cloud_cover
    ).resample_spatial(resolution=spatial_resolution)

    composite = s2_bands.mask(rank_mask.resample_cube_spatial(s2_bands)).aggregate_temporal_period("month", "first")

    if create_opt_tiff:
        output_parameters = {
            "format": "GTiff"
        }
        job = composite.filter_bbox(spatial_extent).execute_batch(output_format="GTiff", output_parameters=output_parameters, filename_prefix="merged_cube")
        job.get_results().download_files(output_period_dir)
        print("Sentinel-2 composite done")
    return composite

def ndvi_eodatacube(c, spatial_extent, start_date, end_date, time_interval,create_opt_tiff, output_period_dir, tif_filename = "composite"):
    period = [start_date, end_date]

    s2cube = c.load_collection(
        "SENTINEL2_L2A",
        temporal_extent=period,
        bands=["B04", "B08", "SCL"],
    )
    red = s2cube.band("B04")
    nir = s2cube.band("B08")
    ndvi = (nir - red) / (nir + red)

    scl = s2cube.band("SCL")
    mask = ~((scl == 4) | (scl == 5))

    # 2D gaussian kernel
    g = scipy.signal.windows.gaussian(11, std=1.6)
    kernel = np.outer(g, g)
    kernel = kernel / kernel.sum()

    # Morphological dilation of mask: convolution + threshold
    mask = mask.apply_kernel(kernel)
    mask = mask > 0.1

    ndvi_masked = ndvi.mask(mask)
    return ndvi_masked

def openeo_eodatacube(c, spatial_extent, start_date, end_date, time_interval, create_opt_tiff, output_period_dir, tif_filename = "composite"):

    period = [start_date, end_date]
    # get a base directory
    base_dir = pathlib.Path(__file__).parent.resolve()
    # load band codes in collections
    with open(base_dir.joinpath("bands.json")) as bdo:
        bands_collections_available = json.load(bdo)

    # get list of required Sentinel-2 band codes
    s2_band_codes = [bands_collections_available["SENTINEL2_L2A"][band_name] for band_name in
                     required_bands_sorted["SENTINEL2_L2A"]]

    scl = c.load_collection(
        "SENTINEL2_L2A",
        temporal_extent=period,
        spatial_extent=spatial_extent,
        bands=["SCL"],
        max_cloud_cover=max_cloud_cover
    )

    cloud_mask = scl.process(
        "to_scl_dilation_mask",
        data=scl,
        kernel1_size=17, kernel2_size=77,
        mask1_values=[2, 4, 5, 6, 7],
        mask2_values=[3, 8, 9, 10, 11],
        erosion_kernel_size=3)

    ndvi_bands = c.load_collection(
        "SENTINEL2_L2A",
        temporal_extent = period,
        spatial_extent = spatial_extent,
        bands = ["B04", "B08", "SCL"],
        max_cloud_cover=95
    )

    ndvi_bands = ndvi_bands.mask(cloud_mask)
    #ndvi_bands = ndvi_bands.process("mask_scl_dilation", data=ndvi_bands, scl_band_name="SCL")

    ndvi = ndvi_bands.ndvi(nir="B08",red="B04")

    def max_ndvi_selection(ndvi):
        max_ndvi = ndvi.max()
        return ndvi.array_apply(lambda x: x != max_ndvi)


    rank_mask = ndvi.apply_neighborhood(
        max_ndvi_selection,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1},
              {'dimension': 't', 'value': "month"}],
        overlap=[]
    )

    rgb_bands = c.load_collection(
        "SENTINEL2_L2A",
        temporal_extent = period,
        spatial_extent = spatial_extent,
        bands = s2_band_codes,
        max_cloud_cover=95
    )

    composite = rgb_bands.mask(rank_mask).aggregate_temporal_period(time_interval, "median")
    composite = composite.apply_dimension(dimension="t", process="array_interpolate_linear")

    if create_opt_tiff:
        output_parameters = {
            "format": "GTiff"
        }
        job = composite.filter_bbox(spatial_extent).execute_batch(output_format="GTiff", output_parameters=output_parameters, filename_prefix="merged_openeo_cube")
        job.get_results().download_files(output_period_dir)
        print("Sentinel-2 composite done")

    return composite
