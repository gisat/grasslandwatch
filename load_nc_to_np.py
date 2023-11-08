import xarray
import numpy as np
import os
import osr
import ogr
import gdal

composite = xarray.open_dataset("/media/jiri/ImageArchive/GW_MLTC_TEST/composite.nc")
print(composite)
composite_path = "/media/jiri/ImageArchive/GW_MLTC_TEST/composite.nc"
site_bbox_filepath = "/media/jiri/ImageArchive/GW_MLTC_TEST/testing_subset/aoi_test.shp"
raster_dst_file = "/media/jiri/ImageArchive/GW_MLTC_TEST/composite_201803.tif"

def create_raster(destfile, driver, proj, data, ulx, uly, pixel_xsize, pixel_ysize, xsize, ysize, data_type,
                  colortable=None, NoDataValue=None):
    Driver = gdal.GetDriverByName(driver)
    tar_ds = Driver.Create(destfile, xsize, ysize, 1, data_type)
    tar_ds.SetGeoTransform((ulx, pixel_xsize, 0, uly, 0, -pixel_ysize))
    tar_ds.SetProjection(proj)
    tar_band = tar_ds.GetRasterBand(1)
    tar_band.WriteArray(data, 0, 0)

    # set NoData value if specified
    if NoDataValue is not None:
        tar_band.SetNoDataValue(NoDataValue)

    # set Color table if specified
    if colortable is not None:
        clrs = gdal.ColorTable()
        for value, rgb in colortable.items():
            clrs.SetColorEntry(int(value), tuple(rgb))
        tar_band.SetRasterColorTable(clrs)

    tar_band.FlushCache()
    tar_band, tar_ds = None, None
    return

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

# composite_array = composite.sel(t="2018-03-01")

rgb_array=composite.to_array(dim="bands").sel(bands=["B04","B03","B02"]).astype(np.float32)

aoi_bounds = get_aoi_bounds_wgs(site_bbox_filepath)

site_ds = ogr.Open(site_bbox_filepath)
site_lyr = site_ds.GetLayer()
site_feat = site_lyr.GetNextFeature()
site_geom = site_feat.GetGeometryRef()
srs_orig = site_lyr.GetSpatialRef()

x, y = list(), list()
for i in range(0, site_geom.GetGeometryCount()):
    linestring = site_geom.GetGeometryRef(i)
    for i in range(0, linestring.GetPointCount()):
        # GetPoint returns a tuple not a Geometry
        point = linestring.GetPoint(i)
        y.append(point[0])
        x.append(point[1])
ulx, uly = min(x), max(y)
lrx = max(x)
print((ulx - lrx) / 10)


# create_raster(raster_dst_file,
#               "GTiff",
#               srs_orig,
#               rgb_array,
#               ulx,
#               uly,
#               20,
#               20,
#               parcel_info["mask_csize"],
#               parcel_info["mask_rsize"],
#               gdal.GDT_Byte)
print(aoi_bounds)