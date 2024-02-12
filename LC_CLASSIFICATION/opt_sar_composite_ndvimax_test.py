
import openeo
import xarray
import numpy as np
import io
import requests

import pyproj
import matplotlib.pyplot as plt
import matplotlib


spatial_extent = {'west': 4.45, 'east': 4.50, 'south': 51.16, 'north': 51.17, 'crs': 'epsg:4326'}
period = ["2017-12-01", "2017-12-31"]


c = openeo.connect("openeo.cloud")
c.authenticate_oidc()

scl = c.load_collection(
    "SENTINEL2_L2A",
    temporal_extent = period,
    bands = ["SCL"],
    max_cloud_cover=95
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
    temporal_extent = ["2022-06-04", "2022-08-01"],
    bands = ["B04", "B08", "SCL"],
    max_cloud_cover=95
)

ndvi_bands = ndvi_bands.mask(cloud_mask)
#ndvi_bands = ndvi_bands.process("mask_scl_dilation", data=ndvi_bands, scl_band_name="SCL")

ndvi = ndvi_bands.ndvi(nir="B08",red="B04")


def max_ndvi_selection(ndvi):
    max_ndvi = ndvi.max()
    return ndvi.array_apply(lambda x:x!=max_ndvi)

rank_mask = ndvi.apply_neighborhood(
        max_ndvi_selection,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1},
              {'dimension': 't', 'value': "month"}],
        overlap=[]
    )


rank_mask.filter_bbox(spatial_extent).execute_batch(out_format="GTiff")



rgb_bands = c.load_collection(
    "SENTINEL2_L2A",
    temporal_extent = period,
    bands = ["B02", "B03","B04"],
    max_cloud_cover=95
)

composite = rgb_bands.mask(rank_mask).aggregate_temporal_period("month","first")

composite.filter_bbox(spatial_extent).execute_batch(out_format="GTiff")

