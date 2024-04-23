import openeo
import time
start = time.time()
spatial_extent = {'west': 11.225, 'south': 47.278, 'east': 11.27, 'north': 47.308}

# establish connection to OpenEO backend
connection = (openeo.connect("openeo.dataspace.copernicus.eu"))
try:
    connection.authenticate_oidc()
except:
    connection.authenticate_oidc_device()

# print(connection.job('j-24040400307948e8914727e3b2a7a5c8').logs())

tcd_collection = connection.load_stac(url="https://s3.waw3-1.cloudferro.com/swift/v1/supportive_data/catalog_laea.json",
                                      # spatial_extent=spatial_extent,
                                      bands=["HRL_TCD"],
                                      temporal_extent=["2024-03-01", "2024-03-31"])


job = tcd_collection.execute_batch(out_format="GTiff")
job.get_results().download_files("./test.tif")
print(job.logs())
end = time.time()
print(end-start)
