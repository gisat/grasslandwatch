import pandas
import csv
from Pathlib import Path


attribute_combinations_filepath = "/home/jtomicek/GISAT/GitHub/grasslandwatch/GRASSLAND_TYPE/SITEID-EUNIS.csv"
src_data_dir = Path("/media/jtomicek/ImageArchive/GW/GT/CSV_phenology/2020/")
boxplot_fields = ["AMPL_1", "EOSD_1", "EOSV_1", "LENGTH_1", "LSLOPE_1", "MAXD_1", "MAXV_1", "MINV_1", "RSLOPE_1", "SOSD_1", "SOSV_1", "SPROD_1", "TPROD_1"]



# read required attribute combinations as list of dicts
with open(attribute_combinations_filepath, "r") as acs:
    attribute_combinations = list(csv.DictReader(acs))

# iterate over the input .csv files and append them into one pandas dataframe
