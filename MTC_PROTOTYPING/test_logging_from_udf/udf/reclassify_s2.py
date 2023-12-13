from openeo.udf import XarrayDataCube
from openeo.udf.debug import inspect



def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
    inspect(message="Hello UDF logging")

    """
    Reflassify values in the datacube.
    """
    # TODO: HOW TO LOG FROM HERE???
    # use inspect https://discuss.eodc.eu/t/printing-or-logging-from-udf/421/5
    inspect(data=[i for i in range(21)], message="_____MYVALIDVALUES_____")

    valid_values = [2, 4, 5, 6, 7]


    array = cube.get_array()
    array.values[array.values == 0] = 1
    for valid_value in valid_values:
        array.values[array.values == valid_value] = 0
    array.values[array.values != 0] = 1
    return cube
