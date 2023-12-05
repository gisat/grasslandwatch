from openeo.udf import XarrayDataCube
from scipy.ndimage import binary_erosion

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    Reflassify values in the datacube.
    """
    array = cube.get_array()
    eroded = binary_erosion(array, iterations=3, border_value=1).astype(array.dtype)
    array[:, :] = eroded
    return cube