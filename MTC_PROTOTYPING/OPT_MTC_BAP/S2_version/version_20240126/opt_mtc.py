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

# periods = [["2020-04-01", "2020-04-30"], ["2020-07-01", "2020-07-31"], ["2020-10-01", "2020-10-31"]]
periods = [["2020-10-01", "2020-10-31"]]
aoi_file = "/media/jiri/ImageArchive/GW/MLTC_TEST/COP4N2K_composite_examples/HU_Hortobagy/vector/HU_AOI.shp"
output_dir = "/media/jiri/ImageArchive/GW/MLTC_TEST/BAP_TEST_20240126/HU_HUHN20002"
required_bands_sorted = {"LANDSAT8_L2": ["ls_blue", "ls_green", "ls_red", "ls_nir08", "ls_swir16", "ls_swir22"],
                         "SENTINEL2_L2A": ["s2_blue", "s2_green", "s2_red", "s2_re01", "s2_re02", "s2_re03", "s2_nir08", "s2_swir16", "s2_swir22"]}
spatial_resolution = 20
max_cloud_cover = 90

#  load UDFs
base_dir = pathlib.Path(__file__).parent.resolve()
reclassify_s2 = openeo.UDF.from_file(base_dir.joinpath("udf", "reclassify_s2.py"))
dilate_invalids = openeo.UDF.from_file(base_dir.joinpath("udf", "dilate_invalids.py"))
sieve_s2 = openeo.UDF.from_file(base_dir.joinpath("udf", "sieve_s2.py"))


for period in periods:
    output_period_dir = os.path.join(output_dir, period[0].replace("-", ""))
    if not os.path.isdir(output_period_dir):
        os.makedirs(output_period_dir)

    # establish connection to OpenEO backend
    c = (openeo.connect("openeo.dataspace.copernicus.eu"))
    # c = (openeo.connect("openeo.cloud"))

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
        max_cloud_cover=max_cloud_cover
    ).resample_spatial(spatial_resolution)

    # # create original valid pixels mask
    # valid_pixels_mask_orig = scl.apply(process=reclassify_s2)
    #
    # # apply sieve filter
    # valid_pixels_mask_sieved = valid_pixels_mask_orig.apply(process=sieve_s2)
    #
    # # dilate invalid pixels area
    # valid_pixels_mask_dilated = valid_pixels_mask_sieved.apply(process=dilate_invalids)
    #
    # mask = valid_pixels_mask_dilated.aggregate_temporal(intervals=[period], reducer="max")
    #
    # job = mask.execute_batch(out_format="GTiff")
    # job.get_results().download_files(output_period_dir)
    # break

    # create a cloud binary to later mask out clouds
    # classification = scl.band("SCL")
    # binary = (classification == 0) | (classification == 1) | (classification == 2) | (classification == 3) | (classification == 5) | (classification == 6) | (classification == 7) | (classification == 8) | (classification == 9) | (classification == 10) | (classification == 11)
    # binary = (classification == 0) | (classification == 1) | (classification == 3) | (classification == 8) | (classification == 9) | (classification == 10) | (classification == 11)

    scl = scl.apply(lambda x: if_(is_nan(x), 0, x))

    score = scl.apply_neighborhood(
        process=openeo.UDF.from_file("udf_score.py"),
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1024}, {'dimension': 'y', 'unit': 'px', 'value': 1024}],
        overlap=[{'dimension': 'x', 'unit': 'px', 'value': 64}, {'dimension': 'y', 'unit': 'px', 'value': 64}]
    )
    score = score.rename_labels('bands', ['score'])
    # score = score.mask(binary)

    # mask Sentinel-2 bands with the derived invalid pixels mask
    # score = score.mask(valid_pixels_mask_dilated)


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

    # mask Sentinel-2 bands with the derived invalid pixels mask
    # s2_bands = s2_bands.mask(valid_pixels_mask_dilated)

    # composite = s2_bands.mask(rank_mask).aggregate_temporal_period("month","first")
    composite = s2_bands.mask(rank_mask.resample_cube_spatial(s2_bands)).aggregate_temporal_period("month", "first")

    job = composite.execute_batch(out_format="GTiff")
    job.get_results().download_files(output_period_dir)
    print("Sentinel-2 composite done")

end = time.time()
print(end-start)