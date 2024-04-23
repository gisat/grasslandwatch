import openeo
import sys
import os
import time

input_data_dir = "/media/jiri/ImageArchive/GW/GT/Prototyping"
output_dir = ("./vegetation_indices_sample.tif")
site_code = "AT3304000"
period = ["2020-06-01", "2020-06-30"]

def vi(mtc_cube, vi_code, output_file_path=None):
    if vi_code.lower() == "ndvi":
        red = mtc_cube.band("B04")
        nir = mtc_cube.band("B8A")
        vi_cube = ((nir - red) / (nir + red)) * 10000
    elif vi_code.lower() == "tcari":
        green = mtc_cube.band("B03") / 10000
        red = mtc_cube.band("B04") / 10000
        re1 = mtc_cube.band("B05") / 10000
        vi_cube = (3 * ((re1 - red) - 0.2 * (re1 - green) * (re1 / red))) * 10000
    elif vi_code.lower() == "nmdi":
        nir = mtc_cube.band("B8A")
        swir1 = mtc_cube.band("B11")
        swir2 = mtc_cube.band("B12")
        vi_cube = ((nir - (swir1 - swir2)) / (nir + (swir1 - swir2))) * 10000
    else:
        return None
    vi_cube = vi_cube.linear_scale_range(-10000, 10000, -10000, 10000)
    if output_file_path is None:
        return vi_cube
    else:
        job = vi_cube.execute_batch(out_format="GTiff")
        job.get_results().download_files(output_file_path)
        return output_file_path


def run(input_data_dir, site_code, vi_codes, output_file_path, period):

    # update pythonpath
    git_dir = __file__.split("grasslandwatch")[0]
    sys.path.append(os.path.join(git_dir, "grasslandwatch"))
    import supportive.helper as helper
    import opt_mtc_prototype_test as s2_opt_mtc

    # aoi file
    aoi_file = os.path.join(input_data_dir, site_code, "Support", "{sitecode}_BBox2.shp".format(sitecode=site_code))

    # get required spatial extent
    spatial_extent = helper.get_spatial_extent_wgs(aoi_file)
    # spatial_extent = {'west': 11.1427023, 'south': 47.2293688, 'east': 11.1560950, 'north': 47.2382297}

    # establish connection to OpenEO backend
    connection = (openeo.connect("openeo.dataspace.copernicus.eu"))
    try:
        connection.authenticate_oidc()
    except:
        connection.authenticate_oidc_device()

    # print(connection.job('j-240322609deb403189b8ae7b26ed3d67').logs())
    #
    # datacube = connection.load_stac("s3://eugwtest/AT3304000_EUDEM_TPI.json")
    # job = datacube.execute_batch(out_format="GTiff")
    # job.get_results()
    # print(datacube)
    # return

    # calculate optical composites
    s2_mtc = s2_opt_mtc.run(connection, spatial_extent, period, output_file_path = None)

    # calculate vegetation indices
    vi_cube_merged = None
    for vi_code in vi_codes:
        vis = vi(s2_mtc, vi_code, output_file_path=None)
        if vi_cube_merged is None:
            vi_cube_merged = vis
        else:
            vi_cube_merged = vi_cube_merged.merge_cubes(vis)

    job = vi_cube_merged.execute_batch(out_format="GTiff")
    job.get_results().download_files(output_file_path)
    return output_file_path

start = time.time()

# run(input_data_dir, site_code, vi_code, output_dir, period)
# run(input_data_dir, site_code, ["NDVI", "NMDI", "TCARI"], output_dir, period)
run(input_data_dir, site_code, ["NDVI"], output_dir, period)

stop = time.time()
seconds = stop - start
seconds_remainder = int(seconds % 60)
minutes = seconds/60
minutes_remainder = int(minutes % 60)
hours = int(minutes/60)
print('Processing time: {:02d}:{:02d}:{:02d} - {} [s]'.format(hours, minutes_remainder, seconds_remainder, seconds))






















"""
import openeo
import xarray
import numpy as np
import io
import requests
import pathlib
import json
from openeo.processes import if_, is_nan
import os

# import panel as p

import pyproj
import matplotlib.pyplot as plt
import matplotlib

import supportive.helper as helper

periods = [["2020-01-01", "2020-01-31"]]
aoi_file = "/media/jiri/ImageArchive/GW/MLTC_TEST/COP4N2K_composite_examples/HU_Hortobagy/vector/HU_AOI.shp"
output_dir = "/media/jiri/ImageArchive/GW/MLTC_TEST/BAP_TEST_20240126/HU_HUHN20002"
required_bands_sorted = {"LANDSAT8_L2": ["ls_blue", "ls_green", "ls_red", "ls_nir08", "ls_swir16", "ls_swir22"],
                         "SENTINEL2_L2A": ["s2_blue", "s2_green", "s2_red", "s2_re01", "s2_re02", "s2_re03", "s2_nir08", "s2_swir16", "s2_swir22"]}
spatial_resolution = 20

for period in periods:
    output_period_dir = os.path.join(output_dir, period[0].replace("-", ""))
    if not os.path.isdir(output_period_dir):
        os.makedirs(output_period_dir)

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
        max_cloud_cover=90 # VITO had 70
    ).resample_spatial(spatial_resolution)

    # create a cloud binary to later mask out clouds
    classification = scl.band("SCL")
    binary = (classification == 3) | (classification == 8) | (classification == 9) | (classification == 10)

    scl = scl.apply(lambda x: if_(is_nan(x), 0, x))

    score = scl.apply_neighborhood(
        process=openeo.UDF.from_file("udf_score_20240126.py"),
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1024}, {'dimension': 'y', 'unit': 'px', 'value': 1024}],
        overlap=[{'dimension': 'x', 'unit': 'px', 'value': 64}, {'dimension': 'y', 'unit': 'px', 'value': 64}]
    )

    score = score.rename_labels('bands', ['score'])
    score = score.mask(binary)

    def max_score_selection(score):
        max_score = score.max()
        return score.array_apply(lambda x:x!=max_score)

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
        max_cloud_cover=90
    ).resample_spatial(spatial_resolution)

    # composite = s2_bands.mask(rank_mask).aggregate_temporal_period("month","first")
    composite = s2_bands.mask(rank_mask.resample_cube_spatial(s2_bands)).aggregate_temporal_period("month","first")

    job = composite.execute_batch(out_format="GTiff")
    job.get_results().download_files(output_period_dir)
    print("Sentinel-2 composite done")
"""