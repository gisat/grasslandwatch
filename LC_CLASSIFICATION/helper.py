try:
    from osgeo import ogr
    from osgeo import osr
    from osgeo import gdal
except:
    import ogr
    import osr
    import gdal

def get_spatial_extent_wgs(file_path):
    """
    get bbox coordinates in WGS84
    """

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
            point = linestring.GetPoint(i)
            lat.append(point[0])
            lon.append(point[1])

    return {"west": min(lon), "south": min(lat), "east": max(lon), "north": max(lat)}

def max_ndvi_selection(ndvi):
    max_ndvi = ndvi.max()
    return ndvi.array_apply(lambda x:x!=max_ndvi)