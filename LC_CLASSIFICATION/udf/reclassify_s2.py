from openeo.udf import XarrayDataCube

def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    """
    Reflassify values in the datacube.
    """
    valid_values = [2, 4, 5, 6, 7]
    array = cube.get_array()
    array.values[array.values == 0] = 1
    for valid_value in valid_values:
        array.values[array.values == valid_value] = 0
    array.values[array.values != 0] = 1
    return cube
