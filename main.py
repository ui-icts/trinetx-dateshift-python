from tkinter import Tk
from tkinter import filedialog
import os
import pandas as pd

id_column = 'patient_id'
target_data_type = 'DATETIME (YYYYMMDD)'
dd_filename = 'datadictionary.xlsx'


# adapted from REDCap
def get_shift_days(hex_id):
    date_shift_max = 10
    sub_len = 8
    dec = int(hex_id[10:][:sub_len], 16)
    return round(dec / pow(10, sub_len) * date_shift_max)


root = Tk()
root.withdraw()

dataset_dir = filedialog.askdirectory(title='Select dataset folder')

print(dataset_dir)

datadict_df = pd.read_excel(os.path.join(dataset_dir, dd_filename))
is_datetime = datadict_df['Data Type'] == target_data_type

date_element_lookup = {}

# build list of datetime columns from data dictionary file
for index, row in datadict_df[is_datetime].iterrows():
    file_name = row['Source File Name']
    element_name = row['Data Element']

    date_element_lookup.setdefault(file_name, []).append(element_name)

# for each csv
for filename in date_element_lookup:
    try:
        df = pd.read_csv(os.path.join(dataset_dir, filename))

        # shift dates by x days (calculated using id column)
        for element in date_element_lookup[filename]:
            df[element] = pd.PeriodIndex(df[element], freq='D')
            shift_days_lookup = df[id_column].apply(get_shift_days)
            df[element] -= shift_days_lookup
    except FileNotFoundError:
        print("'" + filename + "'" + ' not found, skipping')
