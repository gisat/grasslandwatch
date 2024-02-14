from netCDF4 import Dataset
import numpy as np

# Replace 'your_file.nc' with the path to your NetCDF file
file_path = '/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/result.nc'

# Open the NetCDF file
nc = Dataset(file_path, 'r')

# Access a specific variable by name. Replace 'variable_name' with the actual name of your variable
variable_name = 'B03' # Example variable
variable_data = nc.variables[variable_name]

# Convert the variable to a NumPy array
data_array = variable_data[:]

# Now you can use 'data_array' as a NumPy array
print(data_array.shape)

# Don't forget to close the NetCDF file when you're done
nc.close()