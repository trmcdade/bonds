import numpy as np
import pandas as pd
import os
from os import listdir
import csv
import xlrd
import xlwt
import pyexcel as pe
from pyexcel_xlsx import get_data

in_path = 'C:\\Users\\trmcdade\\OneDrive\\Laptop\\Duke\\RA\\Proposals\\Summer 2019 Proposal\\Data\\Bloomberg\\USD\\All xlsm\\'
out_path = 'C:\\Users\\trmcdade\\OneDrive\\Laptop\\Duke\\RA\\Proposals\\Summer 2019 Proposal\\Data\\Bloomberg\\USD\\All csv\\'
clean_path = 'C:\\Users\\trmcdade\\OneDrive\\Laptop\\Duke\\RA\\Proposals\\Summer 2019 Proposal\\Data\\Bloomberg\\USD\\Clean csv\\'

for filename in listdir(in_path):
    fn = filename[0:filename.find('.')]
    print(fn)
    #read in the xlsm
    sheet = pe.get_sheet(file_name = filename)
    #write out the csv
    pe.save_as(array = sheet,
               dest_file_name = out_path + fn + ".csv",
               dest_delimiter = ',')

for filename in listdir(out_path):
    os.chdir(out_path)
    print(filename)
    df = pd.read_csv(filename)
    name  = df.iloc[5, 4]
    ticker = df.iloc[7, 4]
    # get rid of duplciate holder names and portfolio types
    df = df.drop(['Unnamed: 0', 'Unnamed: 1',
                  # 'Unnamed: 3', 'Unnamed: 4', 'Unnamed: 5',
                  # 'Unnamed: 7', 'Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10'
                  ],
                 axis = 1)
    # print(test.iloc[11])
    df.rename(columns=dict(zip(df.columns, df.iloc[11])), inplace=True)
    df = df.iloc[12:]
    df.index = range(df.shape[0])
    df.insert(0, 'Name', name)
    df.insert(0, 'Ticker', ticker)
    # print(df.head(20))
    filename = clean_path + 'clean_' + filename
    df.to_csv(filename, header = True)

# note: summaries don't account for 100% of outstanding debt.
os.chdir(clean_path)
case = pd.read_csv('clean_arg_20y_detail.csv')
case2 = pd.read_csv('clean_arg_20y_summary.csv')
case['% Out'].sum()
case2['% Out'].sum()

# put them all in one master file
os.chdir(clean_path)
filepaths = [g for f in listdir(clean_path) for g in listdir(f) if f.endswith('.csv')]
master = pd.concat(map(pd.read_csv, filepaths))
master.to_csv('master.csv', header = True)
