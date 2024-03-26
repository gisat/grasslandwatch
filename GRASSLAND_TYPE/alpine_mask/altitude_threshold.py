from openeo.udf import XarrayDataCube
import numpy as np

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    Reflassify values in the datacube.
    """
    tree_baseline = context["tree_baseline"]
    array = cube.get_array()
    alpine_zone = np.copy(array)
    alpine_zone[alpine_zone < tree_baseline] = 0
    alpine_zone[alpine_zone != 0] = 1
    array[:, :] = alpine_zone
    return cube