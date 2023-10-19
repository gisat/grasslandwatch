import openeo
import gdal
import ogr
import osr
import os

site_bbox_filepath = "/media/jiri/ImageArchive/GW_MLTC_TEST/EUGW_TestSites/CZ0314123_BBox.shp"
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


# establish connection to OpenEO backend (reuse existing authentication)
connection = openeo.connect("openeo.cloud").authenticate_oidc()

# get site bounds in wgs
aoi_bounds = get_aoi_bounds_wgs(site_bbox_filepath)

# get source satellite images as datacube
datacube = connection.load_collection(
    "SENTINEL2_L2A",
    spatial_extent={"west": aoi_bounds["lon_min"], "south": aoi_bounds["lat_min"], "east": aoi_bounds["lon_max"], "north": aoi_bounds["lat_max"]},
    temporal_extent=["2020-03-01", "2020-03-31"],
    bands=bands_required)
print(datacube)