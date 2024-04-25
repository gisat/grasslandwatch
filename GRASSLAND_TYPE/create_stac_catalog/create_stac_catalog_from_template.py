import os
import rasterio
import json

CATALOG_TEMPLATE = "./catalog_template.json"
ITEM_TEMPLATE = "./item_template.json"
S3_KEY_TEMPLATE = "https://s3.waw3-1.cloudferro.com/swift/v1/supportive_data"


# SET INPUT PARAMETERS
input_raster_dir = "/mnt/gisat_shared/ProjektovaData/EUGW/Prototyping/AT3304000/Support_WGS84"
site_code = "AT3304000"
# product_type = "HRL"
# product_type = "EUNIS"
product_type = "EUDEM"

def get_bbox_and_footprint(raster):
    with rasterio.open(raster) as r:
        bounds = r.bounds
        bbox = [bounds.left, bounds.bottom, bounds.right, bounds.top]
        footprint = [[
            [bounds.left, bounds.bottom],
            [bounds.left, bounds.top],
            [bounds.right, bounds.top],
            [bounds.right, bounds.bottom],
            [bounds.left, bounds.bottom]
        ]]
        return (bbox, footprint)

# compose stac catalog file name
dst_catalog_path = os.path.join(os.path.abspath("."), "catalog_{site_code}_{product_type}.json".format(site_code=site_code,
                                                                                                       product_type=product_type))
dst_catalog_url = "{}/{}".format(S3_KEY_TEMPLATE, os.path.basename(dst_catalog_path))

# iterate over the input raster files and create stac items
# create stac item json files
stac_item_urls = list()

for filename in os.listdir(input_raster_dir):
    if product_type not in filename or not filename.endswith(".tif"):
        continue

    src_raster_filepath = os.path.join(input_raster_dir, filename)

    # get raster bbox and footprint
    bbox, footprint = get_bbox_and_footprint(src_raster_filepath)

    # read item template as dictionary
    with open(ITEM_TEMPLATE, "r") as item_template_ds:
        item_dict = json.load(item_template_ds)

    # modify links to the catalog json
    for i in range(len(item_dict["links"])):
        item_dict["links"][i]["href"] = dst_catalog_url

    # modify id
    product_id = filename.split("_")[-1].replace(".tif", "")
    item_id = "{site_code}_{product_id}".format(site_code=site_code, product_id=product_id)
    item_dict["id"] = item_id

    # modify bbox
    item_dict["bbox"] = bbox

    # modify footprint
    item_dict["geometry"]["coordinates"] = footprint

    # modify asset
    image_url = "{}/{}".format(S3_KEY_TEMPLATE, os.path.basename(src_raster_filepath))
    item_dict["assets"]["image"]["href"] = image_url
    item_dict["assets"]["image"]["eo:bands"][0]["name"] = product_id

    dst_item_abspath = os.path.join(os.path.dirname(dst_catalog_path), "{}.json".format(item_id))
    if os.path.isfile(dst_item_abspath):
        os.remove(dst_item_abspath)
    with open(dst_item_abspath, "w+") as item_dst_ds:
        json.dump(item_dict, item_dst_ds, indent=2)

    # create stac item url
    item_url = "{}/{}".format(S3_KEY_TEMPLATE, os.path.basename(dst_item_abspath))

    stac_item_urls.append(item_url)
    print("Stac item json '{}' has been created.".format(os.path.basename(dst_item_abspath)))

# create stac catalog json file
# read catalog template as dictionary
with open(CATALOG_TEMPLATE, "r") as catalog_template_ds:
    catalog_dict = json.load(catalog_template_ds)

# create links record
catalog_dict["links"][0]["href"] = dst_catalog_url

# iterate over stac item urls
for stac_item_url in stac_item_urls:
    catalog_dict["links"].append({
          "rel": "item",
          "href": stac_item_url,
          "type": "application/json"
        })

if os.path.isfile(dst_catalog_path):
    os.remove(dst_catalog_path)
with open(dst_catalog_path, "w+") as catalog_dst_ds:
    json.dump(catalog_dict, catalog_dst_ds, indent=2)

print("Stac catalog json '{}' has been created.".format(os.path.basename(dst_catalog_path)))

