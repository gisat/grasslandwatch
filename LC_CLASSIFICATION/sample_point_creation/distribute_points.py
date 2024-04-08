
import json
from pathlib import Path
from bisect import bisect_left
from collections import defaultdict
from itertools import accumulate
from random import uniform
import logging
import geopandas as gpd
from osgeo import ogr

from shapely.geometry import LineString, shape, mapping
import fiona
from fiona.crs import from_epsg

log = logging.getLogger(__name__)


TRAINING_POINTS_COUNT = 15000
TRAINING_POLYGON_BUFFER_M = 10
TRAINING_MAX_ITERATIONS = 20000
MINIMUM_CLASS_RATIO = 0.05


def split_multipolygons(features):
    singlepolygon_features = []
    for feat in features:
        if feat["geom"].geom_type == "Polygon":
            singlepolygon_features.append(feat)
        elif feat["geom"].geom_type == "MultiPolygon":
            for polygon in list(feat["geom"].geoms):
                single_polygon_feat = {"id": feat["id"],
                                       "class_id": feat["class_id"],
                                       "geom": polygon,
                                       "epsg":feat["epsg"]}
                singlepolygon_features.append(single_polygon_feat)
    return singlepolygon_features


def save_training_features(features, geometry_type, epsg, gpkg_filepath):
    schema = {
        "geometry": geometry_type,
        "properties": {"id": "int", "class_id": "int"},
    }
    crs = from_epsg(epsg)
    with fiona.open(str(gpkg_filepath), "w", driver="GPKG", schema=schema,crs=crs) as c:
        for feature in features:
            c.write({
                "geometry": mapping(feature["geom"]),
                "properties": {"id": feature["id"],
                               "class_id": feature["class_id"]}
            })
    return gpkg_filepath

class Distribute():

    def apply_buffer(self, features, distance):
        """Applies buffer to all features.
        It returns two feature collections:
        * buffered features, original features with applied buffer;
        * empty features, original features where applying buffer results in empty geometry;"""
        bufferred_features = []
        empty_features = []
        for feature in features:
            new_geom = feature["geom"].buffer(distance)
            if new_geom.is_empty:
                empty_features.append(feature)
            else:
                new_feature = feature.copy()
                new_feature["geom"] = new_geom
                bufferred_features.append(new_feature)
        print(
            "Bufferred features are {:d}, empty features are {:d}.".format(len(bufferred_features),
                                                                           len(empty_features)))
        return bufferred_features, empty_features

    def compute_samples(self, features, sample_count, min_class_sample_ratio):
        """Compute samples for every class based on sample density."""
        # Compute total area per class.
        area_per_class = defaultdict(lambda: 0)
        for feature in features:
            area_per_class[feature["class_id"]] += feature["geom"].area

        total_area = sum(area_per_class.values())
        print("Total of area is {:f}.".format(total_area))
        sample_density = sample_count / total_area
        print("Sample density is {:f} samples per area.".format(sample_density))
        min_class_sample_count = int(sample_count * min_class_sample_ratio)
        print("Minimal sample ratio per class is {:f}.".format(min_class_sample_ratio))
        print("Minimal count of samples per class is {:d}.".format(min_class_sample_count))
        samples_per_class = {}
        for class_id in area_per_class.keys():
            class_sample_count = int(sample_density * area_per_class[class_id])
            print("Computed count of samples for class {:s} is {:d}.".format(repr(class_id), class_sample_count))
            if class_sample_count < min_class_sample_count:
                class_sample_count = min_class_sample_count
                print(
                    "Count of samples for class {:s} is raised to minimal {:d}.".format(repr(class_id),
                                                                                        class_sample_count))
            samples_per_class[class_id] = class_sample_count
        print("Total of samples is {:d}.".format(sum(samples_per_class.values())))
        return samples_per_class

    def allocate_samples(self, features, samples_per_class):
        """Randomly allocate samples to every feature."""
        # Create stripe of features per every class.
        stripes = defaultdict(list)
        for feature in features:
            feature["sample_count"] = 0
            stripes[feature["class_id"]].append(feature)

        # Allocate samples for every class.
        for class_id in stripes.keys():
            accum_fractions = list(accumulate(feature["geom"].area for feature in stripes[class_id]))
            for i in range(samples_per_class[class_id]):
                for i in range(100):
                    random_nr = uniform(0, accum_fractions[-1])
                idx = bisect_left(accum_fractions, random_nr)
                stripes[class_id][idx]["sample_count"] += 1
            print("Samples for class {:s} have been allocated.".format(repr(class_id)))

    def distribute_samples(self, features):
        """Distributes samples to every feature."""
        for feature in features:
            samples = self.distribute_mitchell(feature["geom"], feature["sample_count"])
            feature["sample_geoms"] = samples

    def export_features(features, filepath):
        """Export features as geojson."""
        geojson_features = [{"type": "Feature",
                             "geometry": {"type": "Polygon",
                                          "coordinates": [list(feature["geom"].exterior.coords)]},
                             "properties": {"id": feature["id"],
                                            "class_id": feature["class_id"]}}
                            for feature in features]
        geojson = {"type": "FeatureCollection", "features": geojson_features}
        with open(filepath, "w") as f:
            json.dump(geojson, f)

    def export_samples(self, features, filepath):
        """Export samples as geojson."""
        geojson_features = []
        for feature in features:
            geojson_samples = [{"type": "Feature",
                                "geometry": {"type": "Point",
                                             "coordinates": list(sample_geom.coords)[0]},
                                "properties": {"class_id": feature["class_id"],
                                               "src_id": feature["id"]}}
                               for sample_geom in feature["sample_geoms"]]
            geojson_features.extend(geojson_samples)
        geojson = {"type": "FeatureCollection", "features": geojson_features}
        with open(filepath, "w") as f:
            json.dump(geojson, f)

    def random_point_by_intersection(self, geom):
        """Randomly creates point inside polygon geometry."""
        (xmin, ymin, xmax, ymax) = geom.bounds
        yrand = uniform(ymin, ymax)
        yline = LineString(((xmin, yrand), (xmax, yrand)))
        yline2 = yline.intersection(geom)
        distance = uniform(0, yline2.length)
        if distance > 0:
            point = yline2.interpolate(distance)
            return point
        else:
            return None

    def distribute_mitchell(self, geom, count, candidate_count=10):
        samples = []
        if count == 0:
            return samples

        random_point = self.random_point_by_intersection(geom)
        if random_point is not None:
            samples.append(random_point)

        for i in range(count - 1):
            # Generate several candidate samples to choose from.
            # Every pair consists of:
            #  * candidate sample itself,
            #  * the distance from the candidate sample to the nearest of settled samples.
            candidate_samples = []
            # for i in range(candidate_count):
            #    random_point = random_point_by_intersection(geom)
            #    if random_point is not None:
            #        candidate_samples.append(random_point, min(candidate_sample.disatnce))

            candidate_samples = [(candidate_sample,
                                  min(candidate_sample.distance(s) for s in samples))
                                 for candidate_sample in
                                 (self.random_point_by_intersection(geom) for i in range(candidate_count))]

            # Sort candidate samples by distance to the nearest settled sample.
            candidate_samples.sort(key=lambda s: s[1])

            # Choose the sample farthest of all settled samples.
            samples.append(candidate_samples[-1][0])
        return samples

    def random_point_by_intersection(self, geom):
        """Randomly creates point inside polygon geometry."""
        (xmin, ymin, xmax, ymax) = geom.bounds
        yrand = uniform(ymin, ymax)
        yline = LineString(((xmin, yrand), (xmax, yrand)))
        yline2 = yline.intersection(geom)
        distance = uniform(0, yline2.length)
        if distance > 0:
            point = yline2.interpolate(distance)
            return point
        else:
            return None


    def distribute_mitchell(self, geom, count, candidate_count=10):
        samples = []
        if count == 0:
            return samples

        random_point = self.random_point_by_intersection(geom)
        if random_point is not None:
            samples.append(random_point)

        for i in range(count - 1):
            # Generate several candidate samples to choose from.
            # Every pair consists of:
            #  * candidate sample itself,
            #  * the distance from the candidate sample to the nearest of settled samples.
            candidate_samples = []
            # for i in range(candidate_count):
            #    random_point = random_point_by_intersection(geom)
            #    if random_point is not None:
            #        candidate_samples.append(random_point, min(candidate_sample.disatnce))

            candidate_samples = [(candidate_sample,
                                  min(candidate_sample.distance(s) for s in samples))
                                 for candidate_sample in
                                 (self.random_point_by_intersection(geom) for i in range(candidate_count))]

            # Sort candidate samples by distance to the nearest settled sample.
            candidate_samples.sort(key=lambda s: s[1])

            # Choose the sample farthest of all settled samples.
            samples.append(candidate_samples[-1][0])
        return samples

distribute = Distribute()

def create_training_point(training_polygons_filepath, training_points_filepath, training_column, training_point_count = TRAINING_POINTS_COUNT, training_polygon_buffer = TRAINING_POLYGON_BUFFER_M,
     minimun_class_ratio = MINIMUM_CLASS_RATIO, training_max_iteration = TRAINING_MAX_ITERATIONS):


    training_polygons_ds = ogr.Open(str(training_polygons_filepath))
    ds_epsg = training_polygons_ds.GetLayer().GetSpatialRef().GetAttrValue('AUTHORITY', 1)

    features = []
    i= 0
    for src_feature in fiona.open(str(training_polygons_filepath)):
        i = i + 1
        geometry_shape = shape(src_feature["geometry"])
        features.append({"id": i,
                         "epsg": ds_epsg,
                         "class_id": int(src_feature["properties"][training_column]),
                         "geom": geometry_shape})
    print("Using training polygons from {:s}. Number of polygons: {:d}"
             .format(str(training_polygons_filepath), len(features)))


    # Apply the buffer.
    multipolygon_features, empty_features = distribute.apply_buffer(features, -training_polygon_buffer)

    # Change multipolygons to polygons.
    features = split_multipolygons(multipolygon_features)
    print("Number of polygons after splitting multipolygons: {:d}".format(len(features)))

    # save buffered gpkg
    gpkg_filepath = training_polygons_filepath.parent.joinpath(training_polygons_filepath.name.replace(".gpkg", "_buffered.gpkg"))
    save_training_features(features, "Polygon", ds_epsg, gpkg_filepath)



    ### create training points
    samples_per_class = distribute.compute_samples(features, int(training_point_count), float(minimun_class_ratio))

    # allocate samples per-feature.
    distribute.allocate_samples(features, samples_per_class)

    # distribute samples inside the feature.
    distribute.distribute_samples(features)

    # save the training points to a GeoPackage.
    all_point_samples = []
    id_counter = 0
    for feature in features:
        samples = [{"type": "Feature",
                    "geometry": {"type": "Point",
                                 "coordinates": list(sample_geom.coords)[0]},
                    "properties": {"UID": id_counter + sample_index,
                                   f"{training_column}": feature["class_id"],
                                   "src_id": feature["id"]}}
                   for sample_index, sample_geom in enumerate(feature["sample_geoms"])]
        if len(samples) > 0:
            id_counter = id_counter + len(feature["sample_geoms"])
            all_point_samples.extend(samples)

    # Convert the list of dictionaries to a GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(all_point_samples)
    gdf = gdf.set_crs(epsg=ds_epsg)

    # Write the GeoDataFrame to a GeoPackage file

    gdf.to_file(training_points_filepath, driver='GPKG')

    log.info("Finished creating training points")
    return training_points_filepath


def main():

    training_polygons_filepath = Path("/LC_CLASSIFICATION/sample_point_creation/sample_data/CZ_N2K2018.gpkg")
    training_column = "CODE_1_18"
    TRAINING_POLYGON_BUFFER_M = 10

    training_points_filepath = training_polygons_filepath.parent.joinpath("training_points2.gpkg")

    create_training_point(training_polygons_filepath, training_points_filepath, training_column, TRAINING_POINTS_COUNT, TRAINING_POLYGON_BUFFER_M,
                      MINIMUM_CLASS_RATIO, TRAINING_MAX_ITERATIONS)

    gdf = gpd.read_file(training_points_filepath)

if __name__ == "__main__":
    main()