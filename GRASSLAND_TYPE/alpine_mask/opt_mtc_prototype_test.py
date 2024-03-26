import openeo
from openeo.processes import if_, is_nan
import time

start = time.time()

SPATIAL_RESOLUTION = 20
MAX_CLOUD_COVER = 90

udf_score = openeo.UDF("""
import numpy as np
import time, math
import xarray as xr
import datetime

from scipy.ndimage import distance_transform_cdt
from skimage.morphology import footprints
from skimage.morphology import binary_erosion, binary_dilation
from openeo.udf import XarrayDataCube



def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:

    cube_array: xr.DataArray = cube.get_array()
    cube_array = cube_array.transpose('t', 'bands', 'y', 'x')

    clouds = np.logical_or(np.logical_and(cube_array < 11, cube_array >= 8), cube_array == 3).isel(bands=0)
    # clouds = np.logical_or(cube_array >= 8, cube_array <= 4).isel(bands=0)

    # weights = [2, 0.8, 2]
    # weights = [1, 0.8, 0.5]
    weights = [1, 0, 0]

    # Calculate the Day Of Year score
    times = cube_array.t.dt.day.values  # returns day of the month for each date
    sigma = 5
    mu = 15
    score_doy = 1 / (sigma * math.sqrt(2 * math.pi)) * np.exp(-0.5 * ((times - mu) / sigma) ** 2)
    score_doy = np.broadcast_to(score_doy[:, np.newaxis, np.newaxis],
                                [cube_array.sizes['t'], cube_array.sizes['y'], cube_array.sizes['x']])

    # Calculate the Distance To Cloud score
    # Erode
    # Source: https://github.com/dzanaga/satio-pc/blob/e5fc46c0c14bba77e01dca409cf431e7ef22c077/src/satio_pc/preprocessing/clouds.py#L127
    er = footprints.disk(3)
    di = footprints.disk(30)
    # Define a function to apply binary erosion
    def erode(image, selem):
        return binary_erosion(image, selem)

    def dilate(image, selem):
        return ~binary_dilation(image, selem)

    # Use apply_ufunc to apply the erosion operation
    eroded = xr.apply_ufunc(
        erode,  # function to apply
        clouds,  # input DataArray
        input_core_dims=[['y', 'x']],  # dimensions over which to apply function
        output_core_dims=[['y', 'x']],  # dimensions of the output
        vectorize=True,  # vectorize the function over non-core dimensions
        dask="parallelized",  # enable dask parallelization
        output_dtypes=[np.int32],  # data type of the output
        kwargs={'selem': er}  # additional keyword arguments to pass to erode
    )

    # Use apply_ufunc to apply the erosion operation
    dilated = xr.apply_ufunc(
        dilate,  # function to apply
        eroded,  # input DataArray
        input_core_dims=[['y', 'x']],  # dimensions over which to apply function
        output_core_dims=[['y', 'x']],  # dimensions of the output
        vectorize=True,  # vectorize the function over non-core dimensions
        dask="parallelized",  # enable dask parallelization
        output_dtypes=[np.int32],  # data type of the output
        kwargs={'selem': di}  # additional keyword arguments to pass to erode
    )

    # Distance to cloud = dilation
    d_min = 0
    d_req = 100
    d = xr.apply_ufunc(
        distance_transform_cdt,
        dilated,
        input_core_dims=[['y', 'x']],
        output_core_dims=[['y', 'x']],
        vectorize=True,
        dask="parallelized",
        output_dtypes=[np.int32]
    )
    d = xr.where(d == -1, d_req, d)
    score_clouds = 1 / (1 + np.exp(-0.2 * (np.minimum(d, d_req) - (d_req - d_min) / 2)))

    # Calculate the Coverage score
    score_cov = 1 - clouds.sum(dim='x').sum(dim='y') / (
            cube_array.sizes['x'] * cube_array.sizes['y'])
    score_cov = np.broadcast_to(score_cov.values[:, np.newaxis, np.newaxis],
                                [cube_array.sizes['t'], cube_array.sizes['y'], cube_array.sizes['x']])

    # Final score is weighted average
    score = (weights[0] * score_clouds + weights[1] * score_doy + weights[2] * score_cov) / sum(weights)
    # weights = [1, 0.5]
    # score = (weights[0] * score_clouds + weights[1] * score_cov) / sum(weights)
    score = np.where(cube_array.values[:,0,:,:]==0, 0, score)

    score_da = xr.DataArray(
        score,
        coords={
            't': cube_array.coords['t'],
            'y': cube_array.coords['y'],
            'x': cube_array.coords['x'],
        },
        dims=['t', 'y', 'x']
    )

    score_da = score_da.expand_dims(
        dim={
            "bands": cube_array.coords["bands"],
        },
    )

    score_da = score_da.transpose('t', 'bands', 'y', 'x')

    return XarrayDataCube(score_da)
""")

def run(connection, spatial_extent, temporal_extent, output_file_path = None):

    scl = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent=spatial_extent,
        temporal_extent=temporal_extent,
        bands=["SCL"],
        max_cloud_cover=MAX_CLOUD_COVER
    ).resample_spatial(SPATIAL_RESOLUTION)

    scl = scl.apply(lambda x: if_(is_nan(x), 0, x))

    score = scl.apply_neighborhood(
        process=udf_score,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1024}, {'dimension': 'y', 'unit': 'px', 'value': 1024}],
        overlap=[{'dimension': 'x', 'unit': 'px', 'value': 64}, {'dimension': 'y', 'unit': 'px', 'value': 64}]
    )
    score = score.rename_labels('bands', ['score'])

    def max_score_selection(score):
        max_score = score.max()
        return score.array_apply(lambda x: x != max_score)


    rank_mask = score.apply_neighborhood(
        max_score_selection,
        size=[{'dimension': 'x', 'unit': 'px', 'value': 1}, {'dimension': 'y', 'unit': 'px', 'value': 1},
              {'dimension': 't', 'value': "month"}],
        overlap=[]
    )

    rank_mask = rank_mask.band('score')

    s2_band_codes = ["B02", "B03", "B04", "B05", "B06", "B07", "B8A", "B09", "B11", "B12"]
    s2_bands = connection.load_collection(
        "SENTINEL2_L2A",
        temporal_extent = temporal_extent,
        spatial_extent = spatial_extent,
        bands = s2_band_codes,
        max_cloud_cover=MAX_CLOUD_COVER
    ).resample_spatial(SPATIAL_RESOLUTION)

    composite = s2_bands.mask(rank_mask.resample_cube_spatial(s2_bands)).aggregate_temporal_period("month", "first")

    if output_file_path:
        job = composite.execute_batch(out_format="GTiff")
        job.get_results().download_files(output_file_path)
        return output_file_path
    else:
        return composite