import openeo
import sys
import os
import time
from openeo.udf.debug import inspect
from openeo.processes import array_element, normalized_difference


# input_data_dir = "/media/jiri/ImageArchive/GW/GT/Prototyping"
# output_file_path = "./alpine_test.tif"
# site_code = "AT3304000"
# MNTREG_ID = "alps"
# period = ["2020-05-01", "2020-05-30"]


def calculate_ndvi(data):
    B04 = array_element(data, label="B04")  # array_element takes either an index ..
    B08 = array_element(data, label="B8A")  # or a label

    # ndvi = (B08 - B04) / (B08 + B04) # implement NDVI as formula ..
    ndvi = normalized_difference(B08, B04)  # or use the openEO "normalized_difference" process
    ndvi = ndvi.linear_scale_range(-1, 1, -10000, 10000)
    return ndvi

def ndvi_mask(ndvi):
    return ndvi.array_apply(lambda x: 1 if x > 0.25 else 0)


def run(input_data_dir, site_code, MNTREG_ID, output_file_path):

    # update pythonpath
    git_dir = __file__.split("grasslandwatch")[0]
    sys.path.append(os.path.join(git_dir, "grasslandwatch"))
    import supportive.helper as helper
    import opt_mtc_prototype_test as s2_opt_mtc

    # get regional tree base-line
    regions_tree_baselines = {"alps": 1600,
                              "ape": 1700,
                              "ba_se": 1200,
                              "carp": 1390,
                              "ce_1": 1200,
                              "ce_2": 1200,
                              "e_med": 1500,
                              "frch": 1200,
                              "ib": 1700,
                              "nord": 900,
                              "pyr": 1600,
                              "tr": 1500,
                              "uk": 900,
                              "w_med": 1600}
    # if MNTREG_ID in regions_tree_baselines.keys():
    #     tree_baseline = regions_tree_baselines[MNTREG_ID]
    # else:
    #     return None

    # aoi file
    # aoi_file = os.path.join(input_data_dir, site_code, "Support", "{sitecode}_BBox2.shp".format(sitecode=site_code))

    # get required spatial extent
    # spatial_extent = helper.get_spatial_extent_wgs(aoi_file)
    spatial_extent = {'west': 11.1427023, 'south': 47.2293688, 'east': 11.1560950, 'north': 47.2382297}

    # establish connection to OpenEO backend
    connection = (openeo.connect("openeo.dataspace.copernicus.eu"))
    try:
        connection.authenticate_oidc()
    except:
        connection.authenticate_oidc_device()


    # TODO: THIS PART HAS TO BE REPLACED WITH CORRECT USING OF TCD FOR GENERATING FORREST MASK!!!!
    # TODO: delete opt_mtc_prototype_test.py from this folder!!!
    # calculate ndvi from Sentinel-2 optical composite 2020-06, reclassify it to fake-up forest mask (binary: 1=forest, 0=non-forest)
    s2_mtc = s2_opt_mtc.run(connection, spatial_extent, ["2020-05-01", "2020-05-30"], output_file_path = None)
    s2_mtc_ndvi = s2_mtc.reduce_dimension(reducer=calculate_ndvi, dimension="bands")
    s2_mtc_ndvi_mask = (s2_mtc_ndvi > 0.25)
    # TODO: THIS PART HAS TO BE REPLACED WITH CORRECT USING OF TCD FOR GENERATING FORREST MASK!!!!

    # load Copernicus Global 30 meter Digital Elevation Model as datacube
    eudem_elevation = connection.load_collection(
        collection_id="COPERNICUS_30",
        temporal_extent=["2010-01-01", "2030-12-31"],
        spatial_extent=spatial_extent).resample_cube_spatial(s2_mtc_ndvi_mask, method="bilinear")

    # generate 'above-tree-level' mask
    alpine_zone = (eudem_elevation > 1700)

    # combine forest mask and altitude mask (filter out only the trees above the tree-level baseline)
    forests_alpine_zone_mask = alpine_zone.drop_dimension("t") * s2_mtc_ndvi_mask.drop_dimension("t")

    # get elevation model of forest area in the alpine zone
    alpine_forest_elevation = eudem_elevation.drop_dimension("t") * forests_alpine_zone_mask

    # hear, I am trying to get a mask of forrest pixels with above-average elevation (this is artificial example, for masking
    # of alpine grasslands I will use a slightly different logic of masking, but the technical principle should be the same as in this example
    mean_forest_elevation = alpine_forest_elevation.apply_neighborhood(openeo.processes.mean,
                                                        size=[{'dimension': 't'}],
                                                        overlap=[])

    # hear I mask-out the under-mean elevation pixels from the original forest-elevation layer...
    alpine_forest_elevation_abovemean = alpine_forest_elevation.mask(mean_forest_elevation)
    job = alpine_forest_elevation_abovemean.execute_batch(out_format="GTiff")
    job.get_results().download_files("./alpine_forest_elevation_abovemean.tif")
    print(job.logs())



# run(input_data_dir, site_code, MNTREG_ID, output_file_path)
run(None, None, None, None)