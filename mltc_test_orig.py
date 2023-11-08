import openeo
import xarray
import numpy as np
import io
import requests
import ogr
import osr


import pyproj
import matplotlib.pyplot as plt
import matplotlib




site_bbox_filepath = "/media/jiri/ImageArchive/GW_MLTC_TEST/testing_subset/aoi_test.shp"
# site_bbox_filepath = "/media/jiri/ImageArchive/GW_MLTC_TEST/EUGW_TestSites/ES6110005_BBox.shp"

start_date = "2018-03-01"
end_date = "2018-03-31"
bands_required = ["B02", "B03", "B04", "B05", "B06"]


def get_aoi_bounds_wgs(file_path):
    # get bbox coordinates in WGS84
    site_ds = ogr.Open(file_path)
    site_lyr = site_ds.GetLayer()
    site_feat = site_lyr.GetNextFeature()
    site_geom = site_feat.GetGeometryRef()

    srs_orig = site_lyr.GetSpatialRef()
    srs_dst = osr.SpatialReference()
    srs_dst.ImportFromEPSG(4326)
    transform_wgs = osr.CoordinateTransformation(srs_orig, srs_dst)
    site_geom.Transform(transform_wgs)

    lon, lat = list(), list()
    for i in range(0, site_geom.GetGeometryCount()):
        linestring = site_geom.GetGeometryRef(i)
        for i in range(0, linestring.GetPointCount()):
            # GetPoint returns a tuple not a Geometry
            point = linestring.GetPoint(i)
            lat.append(point[0])
            lon.append(point[1])
    return {"lat_max": max(lat), "lat_min": min(lat), "lon_max": max(lon), "lon_min": min(lon)}

def max_ndvi_selection(ndvi):
    max_ndvi = ndvi.max()
    return ndvi.array_apply(lambda x:x!=max_ndvi)


# establish connection to OpenEO backend (reuse existing authentication)
# c = openeo.connect("openeo.cloud").authenticate_oidc()
c = openeo.connect("openeo.cloud").authenticate_oidc_device()


# get site bounds in wgs
aoi_bounds = get_aoi_bounds_wgs(site_bbox_filepath)

# set temporal extent
temporal_extent = [start_date, end_date]
# temporal_extent = ["2022-06-04", "2022-08-01"]



scl = c.load_collection(
    "SENTINEL2_L2A",
    temporal_extent = temporal_extent,
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
    temporal_extent = temporal_extent,
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

spatial_extent={"west": aoi_bounds["lon_min"], "south": aoi_bounds["lat_min"], "east": aoi_bounds["lon_max"],
                "north": aoi_bounds["lat_max"]}
# spatial_extent = {'west': 4.45, 'east': 4.50, 'south': 51.16, 'north': 51.17, 'crs': 'epsg:4326'}

rank_mask.filter_bbox(spatial_extent).execute_batch("/media/jiri/ImageArchive/GW_MLTC_TEST/mask.nc")


mask_ds = xarray.open_dataset("/media/jiri/ImageArchive/GW_MLTC_TEST/mask.nc")
print(mask_ds)

mask_ds['var'] = mask_ds['var'].where(mask_ds['var']!=129)
mask_ds['var'].plot(vmin=0,vmax=1,col="t",col_wrap=4)



rgb_bands = c.load_collection(
    "SENTINEL2_L2A",
    temporal_extent = temporal_extent,
    bands = ["B02", "B03","B04"],
    max_cloud_cover=95
)

composite = rgb_bands.mask(rank_mask).aggregate_temporal_period("month","first")

job = composite.filter_bbox(spatial_extent).execute_batch(out_format="GTiff")
job.get_results().download_files("/media/jiri/ImageArchive/GW_MLTC_TEST/results")


# composite = xarray.open_dataset("/media/jiri/ImageArchive/GW_MLTC_TEST/composite.nc")
# print(composite)
#
# rgb_array=composite.to_array(dim="bands").sel(bands=["B04","B03","B02"]).astype(np.float32)/10000
# print(rgb_array)
#
# xarray.plot.imshow(rgb_array.isel(t=0),vmin=0,vmax=0.18,rgb="bands",col_wrap=2)















# # create binary cloud masks
# scl = connection.load_collection(
#     "SENTINEL2_L2A",
#     spatial_extent={"west": aoi_bounds["lon_min"], "south": aoi_bounds["lat_min"], "east": aoi_bounds["lon_max"],
#                     "north": aoi_bounds["lat_max"]},
#     temporal_extent=temporal_extent,
#     bands = ["SCL"],
#     max_cloud_cover=95
# )
#
# cloud_mask = scl.process(
#     "to_scl_dilation_mask",
#     data=scl,
#     kernel1_size=17, kernel2_size=77,
#     mask1_values=[2, 4, 5, 6, 7],
#     mask2_values=[3, 8, 9, 10, 11],
#     erosion_kernel_size=3)
#
# # load RED and NIR bands
# ndvi_bands = connection.load_collection(
#     "SENTINEL2_L2A",
#     temporal_extent = temporal_extent,
#     spatial_extent={"west": aoi_bounds["lon_min"], "south": aoi_bounds["lat_min"], "east": aoi_bounds["lon_max"],
#                     "north": aoi_bounds["lat_max"]},
#     bands = ["B04", "B08"],
#     max_cloud_cover=95
# )
#
# # mask them with cloud mask
# ndvi_bands = ndvi_bands.mask(cloud_mask)
#
# # calculate NDVI
# ndvi = ndvi_bands.ndvi(nir="B08",red="B04")
#
# rank_mask = ndvi.apply_neighborhood(
#         max_ndvi_selection,
#         size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1},
#               {'dimension': 't', 'value': "month"}],
#         # size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1}],
#         overlap=[]
#     )
#
# print(rank_mask)
#
# rank_mask.filter_bbox({"west": aoi_bounds["lon_min"], "south": aoi_bounds["lat_min"], "east": aoi_bounds["lon_max"],
#                     "north": aoi_bounds["lat_max"]}).execute_batch("/media/jiri/ImageArchive/GW_MLTC_TEST/mask.nc")
#
#
#
# rgb_bands = connection.load_collection(
#     "SENTINEL2_L2A",
#     temporal_extent = temporal_extent,
#     bands = ["B02", "B03","B04", "B08", "B11", "B12"],
#     max_cloud_cover=95
# )
#
# composite = rgb_bands.mask(rank_mask).aggregate_temporal_period("month","first")
#
# composite.filter_bbox({"west": aoi_bounds["lon_min"], "south": aoi_bounds["lat_min"], "east": aoi_bounds["lon_max"],
#                     "north": aoi_bounds["lat_max"]}).execute_batch("/media/jiri/ImageArchive/GW_MLTC_TEST/composite.nc")
#
# composite = xarray.open_dataset("/media/jiri/ImageArchive/GW_MLTC_TEST/composite.nc")
# print(composite)
#
#
#
# # rank_mask.download_file("/media/jtomicek/ImageArchive/GW_MLTC_TEST/mask.tif")
#
#
#
# # # get source satellite images as datacube
# # datacube = connection.load_collection(
# #     "SENTINEL2_L2A",
# #     spatial_extent={"west": aoi_bounds["lon_min"], "south": aoi_bounds["lat_min"], "east": aoi_bounds["lon_max"], "north": aoi_bounds["lat_max"]},
# #     temporal_extent=["2020-03-01", "2020-03-31"],
# #     bands=bands_required)
# # print(datacube)