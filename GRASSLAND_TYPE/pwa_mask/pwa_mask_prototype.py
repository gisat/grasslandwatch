import openeo
from osgeo import ogr, osr, gdal
import os
import time

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

def run(tpi_threshold, twi_threshold, aoi_shp, output_dir):

    # establish connection to OpenEO backend
    connection = (openeo.connect("openeo.dataspace.copernicus.eu"))
    # connection = (openeo.connect("openeo.vito.be"))
    try:
        connection.authenticate_oidc()
    except:
        connection.authenticate_oidc_device()

    # get aoi extent
    spatial_extent = get_spatial_extent_wgs(aoi_shp)

    # loading of EUDEM TPI TWI topographic indices
    topo_indices = connection.load_stac(url="https://s3.waw3-1.cloudferro.com/swift/v1/supportive_data/catalog_AT3304000_EUDEM.json",
                               spatial_extent=spatial_extent,
                               bands=["TPI", "TWI"],
                               temporal_extent=["2024-03-01", "2024-03-31"])

    # reproject to LAEA
    topo_indices = topo_indices.resample_spatial(resolution=10, projection=3035, method="near")

    # apply threshold to TPI index
    tpi_thresholded = (topo_indices.filter_bands("TPI") < tpi_threshold).drop_dimension("t")

    # apply threshold to TWI index
    twi_thresholded = (topo_indices.filter_bands("TWI") > twi_threshold).drop_dimension("t")

    pwa_mask = tpi_thresholded * twi_thresholded

    job = topo_indices.filter_bands("TPI").execute_batch(out_format="GTiff")
    job.get_results().download_files(os.path.join(output_dir, "tpi.tif"))
    return

start = time.time()
output_dir = "/home/jiri/GISAT/GitHub/grasslandwatch/GRASSLAND_TYPE/alpine_mask"
aoi_shp = "/home/jiri/GISAT/GitHub/grasslandwatch/GRASSLAND_TYPE/alpine_mask/AT3304000_BBox2.shp"
tpi_threshold = 0
twi_threshold = 7
run(tpi_threshold, twi_threshold, aoi_shp, output_dir)
end = time.time()
print(end-start)