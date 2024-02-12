
from pathlib import Path
import openeo
import geopandas as gpd
from shapely.geometry import Point

training_polygons_filepath = Path(
    "/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/CZ_N2K2018.gpkg")
output_dir = training_polygons_filepath.parent

training_points_projected_filepath = training_polygons_filepath.parent.joinpath("training_points_4326.gpkg")

# Assuming your GDF is already defined and in EPSG:4326
gdf = gpd.read_file(training_points_projected_filepath)

# Convert GDF to GeoJSON (if your point is in 'gdf')
point_geojson = gdf.geometry.iloc[0].__geo_interface__

longitude, latitude = 4.465, 51.175
offset = 0.0001  # Small offset to create a tiny bounding box

# Create a minimal bounding box around the point
spatial_extent = {
    "west": longitude - offset,
    "east": longitude + offset,
    "south": latitude - offset,
    "north": latitude + offset
}

####### openeo connection #######
c = openeo.connect("openeo.cloud")
c.authenticate_oidc()

s2_data = c.load_collection(
    "SENTINEL2_L2A",
    spatial_extent=spatial_extent,  # This might need adjustment based on backend support
    temporal_extent=["2022-01-01", "2022-01-31"],
    bands=["B04", "B08"]
)

# Example: Calculate NDVI
ndvi = s2_data.ndvi()

# Aggregate over time, if necessary
ndvi_mean = ndvi.mean_time()


# Execute the processing as a batch job or synchronously, and download the result
result = ndvi_mean.execute_batch(output_format="GTIFF", title="Sentinel composite", filename_prefix="merged_cube")
result.get_results().download_files(str(output_dir))
