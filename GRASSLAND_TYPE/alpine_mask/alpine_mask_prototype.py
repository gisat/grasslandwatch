import openeo

def run(spatial_extent, mntreg_id, tcd_threshold, output_file_path):

    # get regional tree base-line
    regions_tree_baselines = {
        # "alps": 1600,
        "alps": 1800, "ape": 1700, "ba_se": 1200, "carp": 1390, "ce_1": 1200, "ce_2": 1200, "e_med": 1500, "frch": 1200,
        "ib": 1700, "nord": 900, "pyr": 1600, "tr": 1500, "uk": 900, "w_med": 1600}
    if mntreg_id in regions_tree_baselines.keys():
        tree_baseline = regions_tree_baselines[mntreg_id]
    else:
        return None

    # establish connection to OpenEO backend
    connection = (openeo.connect("openeo.dataspace.copernicus.eu"))
    try:
        connection.authenticate_oidc()
    except:
        connection.authenticate_oidc_device()

    # loading of HRL_TCD, reclassification to forest binary mask
    tcd = connection.load_stac(url="https://s3.waw3-1.cloudferro.com/swift/v1/supportive_data/catalog.json",
                               spatial_extent=spatial_extent,
                               bands=["HRL_TCD"],
                               temporal_extent=["2024-03-01", "2024-03-31"])
    forest_mask = (tcd > tcd_threshold)

    # load EUDEM elevation
    eudem_elevation = connection.load_collection(
        collection_id="COPERNICUS_30",
        temporal_extent=["2010-01-01", "2030-12-31"],
        spatial_extent=spatial_extent).resample_cube_spatial(forest_mask, method="bilinear")

    # get 'above-tree-level' mask
    alpine_zone = (eudem_elevation > tree_baseline)

    # combine forest mask and altitude mask (filter out only the trees above the tree-level baseline)
    forests_alpine_zone_mask = alpine_zone.drop_dimension("t").mask(forest_mask.drop_dimension("t"))

    # get elevation model of forest area in the alpine zone
    alpine_forest_elevation = eudem_elevation.drop_dimension("t").mask(forests_alpine_zone_mask)

    # get elevation of the most elevated forest pixel
    get_max = openeo.UDF.from_file("get_max.py")
    max_elevation = alpine_forest_elevation.apply(process=get_max)

    # get the area over the occurrence of the most elevated forest pixel
    alpine_grasslands_zone = (eudem_elevation > max_elevation)

    job = alpine_grasslands_zone.execute_batch(out_format="GTiff")
    job.get_results().download_files(output_file_path)

spatial_extent = {'west': 11.1427023, 'south': 47.2293688, 'east': 11.1560950, 'north': 47.2382297}
MNTREG_ID = "alps"
tcd_threshold = 25
output_file_path = "./alpine_test.tif"
run(spatial_extent, MNTREG_ID, tcd_threshold, output_file_path)
