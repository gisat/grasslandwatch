from openeo.udf import XarrayDataCube
from skimage.morphology import remove_small_holes
import numpy as np

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    Reflassify values in the datacube.
    """
    threshold = context["threshold"]
    array = cube.get_array()
    sieved = np.copy(array)
    sieved = remove_small_holes(sieved.astype(bool), area_threshold=int(threshold), connectivity=1)
    sieved = remove_small_holes(sieved.astype(bool), area_threshold=int(threshold), connectivity=1)
    array[:, :] = sieved
    return cube