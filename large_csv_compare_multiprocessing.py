import os
import pandas as pd
import numpy as np
import multiprocessing as mp
from multiprocessing import Pool, Manager
import csv
import glob
import datetime
import psutil

manager = Manager()
shared_list = manager.list()


def parallelize(df_small, df_large, func, n_cores=2):
    df_large_split = np.array_split(df_large, n_cores)
    pool = Pool(n_cores)
    df_large = pd.concat(pool.map(func, df_large_split))
    pool.close()
    # pool.join()
    return df_large


def compare(df):
    global df_small
    print(f"Process Started with PID: {os.getpid()}")
    for idx in df['domain'].index:
        for value in df_small['Domains']:
            if value in str(df['domain'][idx]):
                print(
                    f"{df['username'][idx]}:{df['password'][idx]}, Domain:{df['domain'][idx]}, Website:{df['website'][idx]}, FIS Domain:{value}")
                shared_list.append([df['username'][idx], df['password']
                                    [idx], df['domain'][idx], df['website'][idx], df['sourcefile'][idx], value])
                with open(filename, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([df['username'][idx], df['password']
                                     [idx], df['domain'][idx], df['website'][idx], df['sourcefile'][idx], value])
    print(f"Process with PID {os.getpid()} is Complete")
    return df


# Main

# Clear Screen
os.system('clear')

# Get Today's Date and Time
timestamp = datetime.datetime.now()
timestamp = timestamp.strftime('%b-%d-%Y')

path = '/home/acer/Downloads/Credentials_Checker/'

extension = 'csv'

# Large List
all_filenames = [i for i in glob.glob(f"{path}CSV_Files/*.{extension}")]
try:
    combined_csv = pd.concat([pd.read_csv(f)
                              for f in all_filenames], sort=False)
except ValueError:
    print("No file found in the folder CSV_Files.")
    exit()
df_large = combined_csv
print("CSV_Files loaded successfully.")

# Small List
all_filenames = [i for i in glob.glob(f"{path}Owned_List/*.{extension}")]
try:
    combined_csv = pd.concat([pd.read_csv(f)
                              for f in all_filenames], sort=False)
except ValueError:
    print("No file found in the folder Owned_List.")
    exit()
df_small = combined_csv
print("Owned_List loaded successfully.")

# Save Filename
filename = f"{path}Output/output_{timestamp}.csv"

if psutil.virtual_memory().percent > 90:
    print("Memory Full! \nExiting.")
    exit()

with open(filename, 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Username', 'Password', 'Domain',
                     'Website', 'Matched FIS Domain'])
print("Output files has been created successfully.")

core_count = mp.cpu_count()-1
print(f"CPU Cores Supported: {core_count}")

data = parallelize(df_small, df_large, compare, core_count)
