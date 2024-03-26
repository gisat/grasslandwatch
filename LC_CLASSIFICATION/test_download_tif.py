import openeo

connection = openeo.connect('openeo.dataspace.copernicus.eu')
connection.authenticate_oidc()

# Define your Area of Interest (AOI) and time period
aoi = {'west': 4.45, 'east': 4.50, 'south': 51.16, 'north': 51.17, 'crs': 'epsg:4326'}

start_date = '2017-05-01'
end_date = '2017-05-15'

# Load the Sentinel-2A L2A data for the specified bands and time period
s2a_data = connection.load_collection(
    "SENTINEL2_L2A",
    spatial_extent=aoi,
    temporal_extent=[start_date, end_date],
    bands=["B02", "B04", "B05", "SCL"]
)
job = s2a_data.filter_bbox(aoi).execute_batch(output_format="GTiff")
# Specify your output file path
job.get_results().download_files("/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation")

print("Download completed.")