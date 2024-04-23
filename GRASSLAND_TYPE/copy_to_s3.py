import os
from pathlib import Path
import subprocess

######################################################
root_folder = Path("/home/eouser/userdoc/src/grasslandwatch/LC_CLASSIFICATION/output/models")

def copy_to_s3(bucket_name, local_path, config_path, s3_path= ""):
    cmd = [
        'rclone',
        'copy',
        '--config', config_path,
        "--log-level=INFO",
        "--no-gzip-encoding",
        str(local_path),
        f'EUGrasslandwatch:{bucket_name}/{s3_path}',
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error executing rclone: {result.stderr}")
    else:
        print(f"Upload {local_path} completed!")

# Example usage:

bucket_name = "models"

files_in_folder = os.listdir(root_folder)
config_path = "/home/jiri/rclone/rclone.conf"
for file_item in files_in_folder:
    local_path = root_folder.joinpath(file_item)
    copy_to_s3(bucket_name, local_path, config_path)
