import openeo

connection = openeo.connect("openeo.cloud")

print(connection.describe_collection("SENTINEL2_L2A"))

# when you want to do new authentication
connection = openeo.connect("openeo.cloud").authenticate_oidc_device()
# reuse existing authentication
connection = openeo.connect("openeo.cloud").authenticate_oidc()

datacube = connection.load_collection(
  "SENTINEL1_GRD",
  spatial_extent={"west": 16.06, "south": 48.06, "east": 16.65, "north": 48.35},
  temporal_extent=["2017-03-01", "2017-04-01"],
  bands=["VV", "VH"]
)

datacube = datacube.min_time()
result = datacube.save_result("GTiff")

job = result.create_job()
#
job.start_and_wait()
job.get_results().download_files("output")


print('here')