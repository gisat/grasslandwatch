from geopandas import GeoDataFrame
from shapely.geometry import Polygon

# Define the GeoJSON data
geojson_data = {
    'type': 'FeatureCollection',
    'features': [
        {
            'id': '15117',
            'type': 'Feature',
            'properties': {'target': 3},
            'geometry': {
                'type': 'Polygon',
                'coordinates': [
                    [
                        [13.892653944948018, 48.753919151154015],
                        [13.892660972225691, 48.75400896171814],
                        [13.892525175735548, 48.754013710397935],
                        [13.892518148701855, 48.75392389982392],
                        [13.892653944948018, 48.753919151154015]
                    ]
                ]
            }
        }
    ]
}

# Convert the GeoJSON data to a GeoDataFrame
gdf = GeoDataFrame.from_features(geojson_data["features"])
gdf.set_crs(epsg=4326, inplace=True)  # Set the coordinate reference system to WGS84

# Save the GeoDataFrame as a GeoPackage
gdf.to_file("/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/feature.gpkg", driver="GPKG")

