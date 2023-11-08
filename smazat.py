import os
import gdal

src_tif_file = "/media/jiri/ImageArchive/GW_MLTC_TEST/testing_subset/aoi_test_MTC_OPT_201803.tif"

ds = gdal.Open(src_tif_file)
prj=ds.GetProjection()