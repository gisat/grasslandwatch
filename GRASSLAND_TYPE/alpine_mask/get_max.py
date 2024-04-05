from openeo.udf import XarrayDataCube
import numpy as np
from openeo.udf.debug import inspect


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    Reflassify values in the datacube.
    """
    array = cube.get_array()
    workingcopy = np.copy(array)
    max_value = np.max(workingcopy)
    return max_value