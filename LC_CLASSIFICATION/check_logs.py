import openeo

# Connect to openEO backend
connection = (openeo.connect("openeo.dataspace.copernicus.eu"))

# Authenticate if necessary
connection.authenticate_oidc()

# Fetch the job
job = connection.job("j-24021470a2304a729a048eab37b16be3")

# Get job information
job_info = job.describe()

# Check the status
print(f"Job status: {job_info}")

# Get logs (if available)
if 'logs' in job_info:
    for log in job_info['logs']:
        print(f"Timestamp: {log['time']}, Level: {log['level']}, Message: {log['message']}")
