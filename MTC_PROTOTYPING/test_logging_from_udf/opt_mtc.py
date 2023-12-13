import openeo
import time
import pathlib
import ogr
import osr

# INPUT PARAMETERS: START
collections = ["SENTINEL2_L2A"]
period = ["2017-12-25", "2017-12-25"]
aoi_file = "/home/jiri/GISAT/GitHub/grasslandwatch/MTC_PROTOTYPING/test_logging_from_udf/test_aoi/AT3304000_BBox.shp"
output_dir = "/home/jiri/GISAT/GitHub/grasslandwatch/MTC_PROTOTYPING/test_logging_from_udf/test_output"
# INPUT PARAMETERS: END


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



def run(collections, period, aoi_file, output_dir):
    """
    A prototype tool for calculating optical composites using the OpenEO platform.
    """

    # get a base directory
    base_dir = pathlib.Path(__file__).parent.resolve()

    #  load UDFs
    reclassify_s2 = openeo.UDF.from_file(base_dir.joinpath("udf", "reclassify_s2.py"))

    # establish connection to OpenEO backend
    c = (openeo.connect("openeo.dataspace.copernicus.eu"))
    try:
        c.authenticate_oidc()
    except:
        c.authenticate_oidc_device()

    # get AOI spatial extent
    spatial_extent = get_spatial_extent_wgs(aoi_file)

    # For Sentinel-2 (if required):
    if "SENTINEL2_L2A" in collections:

        # load SCL layers as datacube
        scl = c.load_collection(
            collection_id="SENTINEL2_L2A",
            temporal_extent=period,
            bands=["SCL"],
            max_cloud_cover=100)
        scl = scl.filter_bbox(spatial_extent).resample_spatial(resolution=20, method="near")

        # create original valid pixels mask
        valid_pixels_mask_orig = scl.apply(process=reclassify_s2)

        job = valid_pixels_mask_orig.execute_batch(out_format="GTiff")
        job.get_results().download_files(output_dir)
        print("Done")
        return

start = time.time()

run(collections, period, aoi_file, output_dir)

stop = time.time()
seconds = stop - start
seconds_remainder = int(seconds % 60)
minutes = seconds/60
minutes_remainder = int(minutes % 60)
hours = int(minutes/60)
print('Processing time: {:02d}:{:02d}:{:02d} - {} [s]'.format(hours, minutes_remainder, seconds_remainder, seconds))
