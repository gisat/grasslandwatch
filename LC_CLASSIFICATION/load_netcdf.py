
import xarray as xr
import pandas as pd


netcdf_file_path = "/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/timeseries.nc"
csv_file_path  = "/home/yantra/gisat/src/grasslandwatch/LC_CLASSIFICATION/sample_point_creation/sample_data/timeseries.csv"




# Load the NetCDF file
ds = xr.open_dataset(netcdf_file_path)

# Initialize a dictionary to hold DataFrames for each variable
dataframes = {}

# Iterate over all variables in the dataset
for var_name in ds.variables:
    # Skip coordinates (dimensions without data)
    if var_name in ds.coords:
        continue

    # Convert the variable to a DataFrame
    df = ds[var_name].to_dataframe()

    # Store the DataFrame in the dictionary
    dataframes[var_name] = df


# Assuming 'dataframes' is your dictionary of DataFrames
# Initialize an empty DataFrame to start with
combined_df = pd.DataFrame()

for var_name, df in dataframes.items():
    # If the combined_df is empty, initialize it with the first variable
    if combined_df.empty:
        combined_df = df
    else:
        # Try to join on the index (common dimensions), this assumes that all DataFrames share the same index
        combined_df = combined_df.join(df, rsuffix=f'_{var_name}')


# Now 'combined_df' contains all variables, each as a separate column
print(combined_df.head())

combined_df.to_csv(csv_file_path)

