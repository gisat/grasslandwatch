import openeo
import gdal
import ogr
import osr
import os

site_bbox_filepath = "/media/jtomicek/ImageArchive/GW_MLTC_TEST/EUGW_TestSites/CZ0314123_BBox.shp"
# site_bbox_filepath = "/media/jiri/ImageArchive/GW_MLTC_TEST/EUGW_TestSites/ES6110005_BBox.shp"

start_date = "2020-03-01"
end_date = "2020-03-31"
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
connection = openeo.connect("openeo.cloud").authenticate_oidc()

# get site bounds in wgs
aoi_bounds = get_aoi_bounds_wgs(site_bbox_filepath)

# set temporal extent
temporal_extent = [start_date, end_date]

# create binary cloud masks
scl = connection.load_collection(
    "SENTINEL2_L2A",
    spatial_extent={"west": aoi_bounds["lon_min"], "south": aoi_bounds["lat_min"], "east": aoi_bounds["lon_max"],
                    "north": aoi_bounds["lat_max"]},
    temporal_extent=temporal_extent,
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

# load RED and NIR bands
ndvi_bands = connection.load_collection(
    "SENTINEL2_L2A",
    temporal_extent = temporal_extent,
    spatial_extent={"west": aoi_bounds["lon_min"], "south": aoi_bounds["lat_min"], "east": aoi_bounds["lon_max"],
                    "north": aoi_bounds["lat_max"]},
    bands = ["B04", "B08"],
    max_cloud_cover=95
)

# mask them with cloud mask
ndvi_bands = ndvi_bands.mask(cloud_mask)

# calculate NDVI
ndvi = ndvi_bands.ndvi(nir="B08",red="B04")

rank_mask = ndvi.apply_neighborhood(
        max_ndvi_selection,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1},
              {'dimension': 't', 'value': "month"}],
        overlap=[]
    )

print(rank_mask)

rank_mask.filter_bbox({"west": aoi_bounds["lon_min"], "south": aoi_bounds["lat_min"], "east": aoi_bounds["lon_max"],
                    "north": aoi_bounds["lat_max"]}).execute_batch("/media/jtomicek/ImageArchive/GW_MLTC_TEST/mask.nc")

# rank_mask.download_file("/media/jtomicek/ImageArchive/GW_MLTC_TEST/mask.tif")



# # get source satellite images as datacube
# datacube = connection.load_collection(
#     "SENTINEL2_L2A",
#     spatial_extent={"west": aoi_bounds["lon_min"], "south": aoi_bounds["lat_min"], "east": aoi_bounds["lon_max"], "north": aoi_bounds["lat_max"]},
#     temporal_extent=["2020-03-01", "2020-03-31"],
#     bands=bands_required)
# print(datacube)