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
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1024}, {'dimension': 'y', 'unit': 'px', 'value': 1024}],
        overlap=[{'dimension': 'x', 'unit': 'px', 'value': 64}, {'dimension': 'y', 'unit': 'px', 'value': 64}]
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
    ).resample_spatial(spatial_resolution)

    composite = s2_bands.mask(rank_mask.resample_cube_spatial(s2_bands)).aggregate_temporal_period("month", "first")

    if create_opt_tiff:
        output_parameters = {
            "format": "GTiff",
            "options": {
                "crs": "EPSG:4326",
            # Assuming the API allows specifying the filename directly
            "filename": tif_filename
            }
        }
        job = composite.filter_bbox(spatial_extent).execute_batch(output_format="GTiff", output_parameters=output_parameters, filename_prefix="merged_cube")
        job.get_results().download_files(output_period_dir)
        print("Sentinel-2 composite done")

    return composite