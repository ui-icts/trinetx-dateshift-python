from tkinter import Tk
from tkinter import filedialog
import os
from shutil import copyfile
import pandas as pd
from tqdm import tqdm

id_column = 'patient_id'
target_data_type = 'DATETIME (YYYYMMDD)'
dd_filename = 'datadictionary.xlsx'
log_file_path = 'log.csv'
log_sample_file_path = 'log_sample.csv'
log_df = pd.DataFrame([], columns=['timestamp', 'message'])
verification_log_dfs = []

# adapted from REDCap
def get_shift_days(hex_series):
    hex_series = hex_series.str.slice(start=10, step=4)
    hex_series = hex_series.apply(int, base=16)
    shift_days = round(hex_series / pow(10, 8))
    return shift_days


def add_to_log(string, print_to_console=True):
    log_df.loc[len(log_df.index)] = [pd.Timestamp.today(), string]

    if print_to_console:
        print(string)


root = Tk()
root.withdraw()

dataset_dir = filedialog.askdirectory(title='Select dataset folder')
shifted_dir = os.path.join(dataset_dir, 'shifted')

root.update()
root.destroy()

try:
    os.mkdir(shifted_dir)
except OSError as error:
    add_to_log(error)

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

        log_str = "Processing '" + filename + "'..."
        print(log_str, end="")
        add_to_log(log_str, False)

        if filename in date_element_lookup.keys():
            try:
                df = pd.read_csv(source_file, parse_dates=date_element_lookup[filename])

                if len(df) > 0:
                    add_to_log("found " + str(len(date_element_lookup[filename])) + " date column(s), shifting...")

                    # shift dates by x days (calculated using id column)
                    for date_element in date_element_lookup[filename]:
                        shift_days = get_shift_days(df[id_column])

                        data = [df[id_column], df[date_element].copy(), shift_days]

                        df[date_element] = tqdm(
                            df[date_element] - pd.to_timedelta(shift_days, 'D'),
                            desc=date_element
                        )

                        data.append(df[date_element])
                        headers = ['patient_id', 'original_date', 'shift_days', 'new_date']

                        # log first 10 dateshifts for validation
                        verification_log_dfs.append(pd.concat(data, axis=1, keys=headers).head(10))

                    # save shifted csv
                    add_to_log("Saving shifted file to " + shifted_file)
                    df.to_csv(shifted_file, index=False, header=True, date_format='%Y%m%d')
                else:
                    add_to_log('zero rows found, skipping')

            except FileNotFoundError:
                add_to_log('file not found, skipping')
        else:
            # no dates to shift, just copy file
            add_to_log('no date columns found, copying as-is')
            copyfile(source_file, shifted_file)

log_df.to_csv(log_file_path, index=False, header=True, date_format='%Y-%m-%d %H:%M:%S')
pd.concat(verification_log_dfs).to_csv(log_sample_file_path, index=False, header=True, date_format='%Y-%m-%d')

