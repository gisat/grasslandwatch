import pandas as pd
import csv
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

attribute_combinations_filepath = "/home/jiri/GISAT/GitHub/grasslandwatch/GRASSLAND_TYPE/SITEID-EUNIS.csv"
src_data_dir = Path("/media/jiri/ImageArchive/GW/GT/CSV_phenology/2020")
boxplot_fields = ["AMPL_1", "EOSD_1", "EOSV_1", "LENGTH_1", "LSLOPE_1", "MAXD_1", "MAXV_1", "MINV_1", "RSLOPE_1", "SOSD_1", "SOSV_1", "SPROD_1", "TPROD_1"]



# read required attribute combinations as list of dicts
with open(attribute_combinations_filepath, "r") as acs:
    attribute_combinations = list(csv.DictReader(acs))

ident_fields = list(attribute_combinations[0].keys())

# iterate over the input .csv files and append them into the pandas dataframe
accumulated_data = list()
fields_required = ident_fields + boxplot_fields
for src_csv_path in src_data_dir.iterdir():
    if src_csv_path.suffix != ".csv":
        continue

    # get the csv file content as list of dicts
    with open(src_csv_path, "r") as csv_ds:
        # accumulated_data += list(csv.DictReader(csv_ds, quoting=csv.QUOTE_NONNUMERIC))
        accumulated_data += list(csv.DictReader(csv_ds))

# read the accumulated data as pandas dataframe
df = pd.DataFrame(accumulated_data)

# delete unnecessary columns
colnames_to_delete = list(set(df.columns.values) - set(fields_required))
df = df.drop(colnames_to_delete, axis=1)

# iterate over specified columns (variables for box-plots determination) and attribute combinations (rows)
for boxplot_variable_name in boxplot_fields:

    # create subplots layout
    fig, axs = plt.subplots(2, 28, figsize=(15, 2))
    fig.suptitle(boxplot_variable_name, fontsize="x-large")

    # hide the first row of subplots
    for i in range(28):
        axs[1, i].axis('off')

    i = 0
    for attribute_combination in attribute_combinations:
        x_label = "{}_{}".format(attribute_combination["SITE_ID"], attribute_combination["EUNIS"])
        boxplot_column = df[(df['SITE_ID'] == attribute_combination['SITE_ID']) | (df['EUNIS'] == attribute_combination['EUNIS'])].loc[:, boxplot_variable_name].to_list()
        boxplot_column = [int(i) for i in boxplot_column if i != '']

        # axs[0, 0].boxplot(boxplot_column, 0, '')
        if i == 0:
            axs[0, i].boxplot(boxplot_column, 0, '')
            axs[0, i].set_xlabel(x_label, rotation = 90)
        else:

        # for i in range(1, 32):
            # axs[0, i].boxplot(boxplot_column, 0, '')
            axs[0, i].boxplot(boxplot_column, 0, '')
            # axs[1, i].ylabel('SITE_EUNIS_xxx', rotation = 90)
            axs[0, i].set_xlabel(x_label, rotation = 90)
            axs[0, i].axes.get_yaxis().set_visible(False)

        i += 1

        # plt.boxplot(boxplot_column)
    plt.show()

    # df = pd.read_csv("/media/jiri/ImageArchive/GW/GT/CSV_phenology/test/AT3304000_2020.csv")
# print(df.head())
# print(set(df.columns.values) - set(fields_required))
