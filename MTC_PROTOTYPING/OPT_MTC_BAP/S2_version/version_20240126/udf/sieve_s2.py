from openeo.udf import XarrayDataCube
from skimage.morphology import remove_small_holes
import numpy as np

CLOUD_SIEVE_THRESHOLD_M2 = 4000
PIXEL_SIZE = 20

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    Reflassify values in the datacube.
    """
    array = cube.get_array()
    sieved = np.copy(array)
    sieve_threshold_pixels = int(float(CLOUD_SIEVE_THRESHOLD_M2) / float(PIXEL_SIZE ** 2))
    sieved = remove_small_holes(sieved.astype(bool), area_threshold=sieve_threshold_pixels, connectivity=2)
    array[:, :] = sieved
    return cube