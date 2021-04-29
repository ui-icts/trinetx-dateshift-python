from tkinter import Tk
from tkinter import filedialog
import os
from shutil import copyfile
import pandas as pd
from tqdm import tqdm

id_column = 'patient_id'
target_data_type = 'DATETIME (YYYYMMDD)'
dd_filename = 'datadictionary.xlsx'


# adapted from REDCap
def get_shift_days(hex_series):
    hex_series = hex_series.str.slice(start=10, step=4)
    hex_series = hex_series.apply(int, base=16)
    shift_days = round(hex_series / pow(10, 8))
    return shift_days


root = Tk()
root.withdraw()

dataset_dir = filedialog.askdirectory(title='Select dataset folder')
shifted_dir = os.path.join(dataset_dir, 'shifted')

root.update()
root.destroy()

try:
    os.mkdir(shifted_dir)
except OSError as error:
    print(error)

datadict_df = pd.read_excel(os.path.join(dataset_dir, dd_filename))
is_datetime = datadict_df['Data Type'] == target_data_type

date_element_lookup = {}

# build list of datetime columns from data dictionary file
for index, row in datadict_df[is_datetime].iterrows():
    file_name = row['Source File Name']
    element_name = row['Data Element']

    date_element_lookup.setdefault(file_name, []).append(element_name)

# iterate through all csv files
for entry in os.scandir(dataset_dir):
    if entry.path.endswith(".csv") and entry.is_file():
        filename = entry.name
        source_file = os.path.join(dataset_dir, filename)
        shifted_file = os.path.join(shifted_dir, filename)

        print("Processing '" + filename + "'...", end="")

        if filename in date_element_lookup.keys():
            try:
                df = pd.read_csv(source_file, parse_dates=date_element_lookup[filename])

                if len(df) > 0:
                    print("found " + str(len(date_element_lookup[filename])) + " date column(s), shifting...")

                    # shift dates by x days (calculated using id column)
                    for date_element in date_element_lookup[filename]:
                        df[date_element] = tqdm(
                            df[date_element] - pd.to_timedelta(get_shift_days(df[id_column]), 'D'),
                            desc=date_element
                        )

                    # save shifted csv
                    print("Saving shifted file to " + shifted_file)
                    df.to_csv(shifted_file, index=False, header=True, date_format='%Y%m%d')
                else:
                    print('zero rows found, skipping')

            except FileNotFoundError:
                print('file not found, skipping')
        else:
            # no dates to shift, just copy file
            print('no date columns found, copying as-is')
            copyfile(source_file, shifted_file)
