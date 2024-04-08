import openeo
from openeo.extra.spectral_indices.spectral_indices import compute_and_rescale_indices
import openeo.processes as eop

def timesteps_as_bands(base_features):
    band_names = [band + "_m" + str(i+1) for band in base_features.metadata.band_names for i in range(12)]
    result =  base_features.apply_dimension(
        dimension='t', 
        target_dimension='bands', 
        process=lambda d: eop.array_create(data=d)
    )
    return result.rename_labels('bands', band_names)

def compute_statistics(base_features):
    """
    Computes  MEAN, STDDEV, MIN, P25, MEDIAN, P75, MAX over a datacube.
    """
    def computeStats(input_timeseries):
        result = eop.array_concat(
            input_timeseries.mean(),
            input_timeseries.sd()
        )
        result = eop.array_concat(result, input_timeseries.min())
        result = eop.array_concat(result, input_timeseries.quantiles(probabilities=[0.25]))
        result = eop.array_concat(result, input_timeseries.median())
        result = eop.array_concat(result, input_timeseries.quantiles(probabilities=[0.75]))
        result = eop.array_concat(result, input_timeseries.max())
        return result
    
    stats = base_features.apply_dimension(dimension='t', target_dimension='bands', process=computeStats)
    all_bands = [band + "_" + stat for band in base_features.metadata.band_names for stat in ["mean", "stddev", "min", "p25", "median", "p75", "max"]]
    return stats.rename_labels('bands', all_bands)

def get_s1_features(
        s1_datacube
) -> openeo.DataCube:
    s1 = s1_datacube.linear_scale_range(0, 30, 0,30000)
    s1_month = s1.aggregate_temporal_period(period="month", reducer="mean")

    s1_month = s1_month.apply_dimension(dimension="t", process="array_interpolate_linear")

    s1_features = timesteps_as_bands(s1_month)
    return s1_features

def get_s2_features(
        s2_datacube,
        s2_list,
        s2_index_dict,
) -> openeo.DataCube:
    # TODO compare with BAP or NDVIweighted
    s2 = s2_datacube.process("mask_scl_dilation", data=s2_datacube, scl_band_name="SCL").filter_bands(s2_datacube.metadata.band_names[:-1])

    indices = compute_and_rescale_indices(s2, s2_index_dict, append=False)
    idx_dekad = indices.aggregate_temporal_period("dekad", reducer="mean")
    idx_stats = compute_statistics(idx_dekad)

    s2_montly = s2.filter_bands(s2_list).aggregate_temporal_period("month", reducer="mean")
    s2_montly = s2_montly.apply_dimension(dimension="t", process="array_interpolate_linear")
    s2_features = timesteps_as_bands(s2_montly).merge_cubes(idx_stats)
    return s2_features

def preprocess_features(
        s2_datacube,
        s1_datacube,
) -> openeo.DataCube:
    s2_list = ["B02", "B03", "B04", "B08", "B11", "B12"]
    s2_index_dict = {
        "collection": {
            "input_range": [0, 8000],
            "output_range": [0, 30_000]
        },
        "indices": {
            "NDVI": {"input_range": [-1,1], "output_range": [0, 30_000]}
        }
    }
    
    s2_features = get_s2_features(s2_datacube, s2_list, s2_index_dict)
    s1_features = get_s1_features(s1_datacube)
    # TODO add topopraphic features: elevation, slope and aspect 

    features = s2_features #.merge_cubes(s1_features)
    return features