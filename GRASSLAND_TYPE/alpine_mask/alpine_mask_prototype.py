import shutil

import openeo
import geopandas as gpd
from osgeo import ogr, osr, gdal
import subprocess
import os
import numpy as np
import json
import time

def shp_to_polygon(path:str) -> dict:

    gdf = gpd.read_file(path)
    geometries = eval(gdf.to_crs('epsg:4326').geometry.to_json())
    return(geometries)

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

def filter_grassland_polygons_by_altitude(eudem_file, forest_max_altitude_file, input_zones_file, output_zones_file):

    # read the data file as numpy array
    eudem_ds = gdal.Open(eudem_file)
    eudem_array = eudem_ds.GetRasterBand(1).ReadAsArray()

    # get eudem file extent
    geoTransform = eudem_ds.GetGeoTransform()
    minx = geoTransform[0]
    maxy = geoTransform[3]
    maxx = minx + geoTransform[1] * eudem_ds.RasterXSize
    miny = maxy + geoTransform[5] * eudem_ds.RasterYSize

    # vectorize src zones
    zones_vector_tmp = input_zones_file.replace(".tif", ".gpkg")
    if os.path.isfile(zones_vector_tmp):
        os.remove(zones_vector_tmp)
    vectorize_cmd = ["gdal_polygonize.py", input_zones_file, zones_vector_tmp]
    subprocess.check_output(vectorize_cmd)

    # add 'object_id' field
    add_cmd = ["ogrinfo", zones_vector_tmp, "-sql", "ALTER TABLE out ADD COLUMN object_id integer"]
    subprocess.check_output(add_cmd)

    # fill 'object_id' field
    fill_cmd = ["ogrinfo", zones_vector_tmp, "-dialect", "SQLite", "-sql", "UPDATE out set object_id = rowid"]
    subprocess.check_output(fill_cmd)

    # rasterize the zones, burn object_id value
    rasterize_cmd = ["gdal_rasterize", "-a", "object_id", "-tr", "10", "10", "-te", str(minx), str(miny), str(maxx), str(maxy), zones_vector_tmp, output_zones_file]
    subprocess.check_output(rasterize_cmd)

    # get min and max value from rasterized zones layer
    zones_ras_ds = gdal.Open(output_zones_file, 1)
    zones_ras_band = zones_ras_ds.GetRasterBand(1)
    zones_ras_array = zones_ras_band.ReadAsArray()
    zone_id_min, zone_id_max = int(np.nanmin(zones_ras_array)), int(np.nanmax(zones_ras_array))

    # create an empty array for writing zonal maxima
    zonal_max_array = np.zeros(zones_ras_array.shape)

    for zone_id in range(zone_id_min, zone_id_max + 1):
        zones_ras_array_copy = np.copy(zones_ras_array)
        eudem_array_copy = np.copy(eudem_array)
        zones_ras_array_copy[zones_ras_array_copy != zone_id] = 0
        zones_ras_array_copy[zones_ras_array_copy != 0] = 1
        zonal_max = int(np.nanmax(eudem_array_copy * zones_ras_array_copy))
        zones_ras_array_copy[zones_ras_array_copy != 0] = zonal_max
        zonal_max_array += zones_ras_array_copy
        del zones_ras_array_copy

    # read forest max altitude raster
    forest_max_alt_ds = gdal.Open(forest_max_altitude_file)
    forest_max_alt_array = forest_max_alt_ds.GetRasterBand(1).ReadAsArray()

    zonal_max_array[zonal_max_array > forest_max_alt_array] = 1
    zonal_max_array[zonal_max_array != 1] = 0

    zones_ras_band.WriteArray(zonal_max_array)
    zones_ras_ds, zones_ras_band = None, None
    return output_zones_file

def run(mntreg_id, tcd_threshold, aoi_shp, output_dir, working_dir):

    # get regional tree base-line
    regions_tree_baselines = {
        "alps": 1600, "ape": 1700, "ba_se": 1200, "carp": 1390, "ce_1": 1200, "ce_2": 1200, "e_med": 1500, "frch": 1200,
        "ib": 1700, "nord": 900, "pyr": 1600, "tr": 1500, "uk": 900, "w_med": 1600}
    if mntreg_id in regions_tree_baselines.keys():
        tree_baseline = regions_tree_baselines[mntreg_id]
    else:
        return None

    # establish connection to OpenEO backend
    connection = (openeo.connect("openeo.dataspace.copernicus.eu"))
    # connection = (openeo.connect("openeo.vito.be"))
    try:
        connection.authenticate_oidc()
    except:
        connection.authenticate_oidc_device()

    # get the aoi geometry
    aoi_geom = shp_to_polygon(aoi_shp)

    # get aoi extent
    spatial_extent = get_spatial_extent_wgs(aoi_shp)

    # loading of HRL_TCD, reclassification to forest binary mask
    tcd = connection.load_stac(url="https://s3.waw3-1.cloudferro.com/swift/v1/supportive_data/catalog.json",
                               spatial_extent=spatial_extent,
                               bands=["HRL_TCD"],
                               temporal_extent=["2024-03-01", "2024-03-31"])

    # reproject to LAEA
    tcd = tcd.resample_spatial(resolution=10, projection=3035, method="near")
    forest_mask = (tcd > tcd_threshold)
    forest_mask = forest_mask.drop_dimension("t")
    non_forest_mask = (forest_mask == 0)

    # load EUDEM elevation
    eudem_elevation = connection.load_collection(
        collection_id="COPERNICUS_30",
        temporal_extent=["2010-01-01", "2030-12-31"],
        bands=["DEM"],
        spatial_extent=spatial_extent)

    eudem_elevation = eudem_elevation.resample_cube_spatial(tcd)

    # get 'above-tree-level' mask
    initial_alpine_zone = (eudem_elevation > tree_baseline)
    initial_alpine_zone = initial_alpine_zone.drop_dimension("t")

    # combine forest mask and altitude mask (filter out only the trees above the tree-level baseline)
    forests_alpine_zone = forest_mask * initial_alpine_zone

    # clip the eudem with the forest alpine zone mask
    forest_eudem = eudem_elevation * forests_alpine_zone

    # set the new band label
    forest_eudem = forest_eudem.reduce_dimension(dimension="bands", reducer="max")
    forest_eudem = forest_eudem.add_dimension(name="bands", label="forest_eudem", type="bands")

    # combine non_forest mask and altitude mask (filter out only the grassland pixels above the tree-level baseline)
    grasslands_alpine_zones = non_forest_mask * initial_alpine_zone

    # set the new band label
    grasslands_alpine_zones = grasslands_alpine_zones.reduce_dimension(dimension="bands", reducer="max")
    grasslands_alpine_zones = grasslands_alpine_zones.add_dimension(name="bands", label="grasslands_alpine_mask", type="bands")

    # apply sieve filter to forests alpine zone mask
    sieve = openeo.UDF.from_file("./sieve.py", context={"from_parameter": "context"})
    grasslands_alpine_zones_sieved = grasslands_alpine_zones.apply(process=sieve, context={"threshold": 100})

    # clip the eudem with the grassland alpine zone mask
    grassland_eudem = eudem_elevation * grasslands_alpine_zones_sieved

    # set the new band label
    grassland_eudem = grassland_eudem.reduce_dimension(dimension="bands", reducer="max")
    grassland_eudem = grassland_eudem.add_dimension(name="bands", label="grassland_eudem", type="bands")

    # Get the maximum altitude of any forest pixel
    forest_max_altitude_vec = forest_eudem.aggregate_spatial(geometries=aoi_geom, reducer='max')
    forest_max_eltitude_ras = forest_max_altitude_vec.vector_to_raster(eudem_elevation)

    """
    # THE OPENEO based approach:

    # vectorize the grassland alpine zones layer
    grasslands_alpine_zones_vec = grasslands_alpine_zones_sieved.raster_to_vector()
    job_vectorize = grasslands_alpine_zones_vec.execute_batch(out_format="JSON")
    job_vectorize.get_results().download_files(os.path.join(output_file_path,"grasslands_alpine_zones_vec"))

    data = json.load(open(os.path.join(output_file_path, "grasslands_alpine_zones_vec", "result.json")))

    features_list = list()
    for c in range(len(data)):
        features_list.append({'id': str(c),
                              'type': 'Feature',
                              'properties': {},
                              'geometry': {
                                  'type': 'Polygon',
                                  'coordinates': data[c]["coordinates"]}})

    grasslands_alpine_geom = {
        "type": "FeatureCollection",
        "features": features_list
    }
    
    # Calculate zonal max altitude for grassland polygons
    grasslands_zonal_altitude = grassland_eudem.aggregate_spatial(geometries=grasslands_alpine_geom, reducer='max')
    grasslands_zonal_altitude_ras = grasslands_zonal_altitude.vector_to_raster(eudem_elevation)
    
    # Filter grassland polygons with the max altitude greater than the maximum altitude of any forest pixel 
    final_alpine_grasslands_mask = ((grasslands_elevation_zonal_ras / forest_max_eltitude_ras) > 1)
    
    # export resulting grasslands alpine zones mask as GeoTiff.
    job_final = grasslands_alpine_zones_sieved.execute_batch(out_format="GTiff")
    job_final.get_results().download_files(os.path.join(output_file_path, "grasslands_alpine_zones_final"))
    """

    # The local system based approach:

    # export grasslands alpine zones mask and grassland eudem as GeoTiffs...
    job1 = grasslands_alpine_zones_sieved.execute_batch(out_format="GTiff")
    job1.get_results().download_files(os.path.join(working_dir, "grasslands_alpine_zones_sieved"))

    grassland_eudem = grassland_eudem.drop_dimension("t")
    job2 = grassland_eudem.execute_batch(out_format="GTiff")
    job2.get_results().download_files(os.path.join(working_dir, "grassland_eudem"))

    forest_max_eltitude_ras = forest_max_eltitude_ras.drop_dimension("t")
    job3 = forest_max_eltitude_ras.execute_batch(out_format="GTiff")
    job3.get_results().download_files(os.path.join(working_dir, "forest_max_eltitude_ras"))

    grassland_eudem_file = os.path.join(working_dir, "grassland_eudem", "openEO.tif")
    forest_max_altitude_file = os.path.join(working_dir, "forest_max_eltitude_ras", "openEO.tif")
    input_zones_file = os.path.join(working_dir, "grasslands_alpine_zones_sieved", "openEO.tif")
    output_zones_file = os.path.join(output_dir, "grasslands_alpine_zones_final.tif")

    filter_grassland_polygons_by_altitude(grassland_eudem_file,
                                          forest_max_altitude_file,
                                          input_zones_file,
                                          output_zones_file)

    shutil.rmtree(working_dir)




start = time.time()
# spatial_extent = {'west': 11.1427023, 'south': 47.2293688, 'east': 11.1560950, 'north': 47.2382297}
MNTREG_ID = "alps"
tcd_threshold = 0
output_dir = "/home/jiri/GISAT/GitHub/grasslandwatch/GRASSLAND_TYPE/alpine_mask"
working_dir = "/home/jiri/GISAT/GitHub/grasslandwatch/GRASSLAND_TYPE/tmp"
aoi_shp = "/home/jiri/GISAT/GitHub/grasslandwatch/GRASSLAND_TYPE/alpine_mask/AT3304000_BBox2.shp"
run(MNTREG_ID, tcd_threshold, aoi_shp, output_dir, working_dir)
end = time.time()
print(end-start)