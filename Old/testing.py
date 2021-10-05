import urllib.request # what we use to download html into python
# import gzip # for opening compressed files (.gz)
# from bs4 import BeautifulSoup # for parsing the html once we download
# from time import sleep # for pausing the script so we don't overload servers
from tqdm import tqdm # useful little package for creating progress bars
from datetime import datetime, timedelta, date # for managing date formats
import re # for regular expressions
import pandas as pd
import numpy as np
import os

# set working directory
datapath = 'C:\\Users\\trmcdade\\OneDrive\\Laptop\\Duke\\RA\\Proposals\\Summer 2019 Proposal\\Data'
os.chdir(datapath)
# os.getcwd()

#read csv
df = pd.read_csv('WB_IDS_Data.csv')

# rename columns
old_names = df.columns.values[4:22]
years = list(range(2001,2019))
years = [str(year) for year in years]
df.rename(columns=dict(zip(old_names, years)), inplace=True)
data = pd.melt(df, id_vars = ['Country Name', 'Country Code', 'Series Name', 'Series Code'])
data.columns.values[4] = 'Year'
data = data[['Country Name', 'Series Name', 'Year', 'value']]

out = pd.DataFrame(columns = ['A'], data = [[0]])
for country in data['Country Name'].unique():
    a = data[data['Country Name'] == country]
    b = a.pivot(index = 'Year', columns = 'Series Name', values = 'value')
    name = pd.DataFrame([country for i in range(len(data['Year'].unique()))])
    country_output = pd.concat([name, b.reset_index(drop=True)], axis=1, ignore_index=True)
    if out.count == 1:
        out = country_output
    else:
        out = out.append(country_output, ignore_index = True)

out.head(100)

com = data[data['Series Name'].str.contains("Commitments,", regex = False) == True]
pub = data[data['Series Name'] == 'Commitments, official creditors (COM, current US$)']
priv = data[data['Series Name'] == 'Commitments, private creditors (COM, current US$)']

com.columns
com['Series Name'].unique()
com[com['Series Name'] == 'Commitments, official creditors (COM, current US$)']

test = pd.melt(com, id_vars = ['Country Name', 'Country Code', 'Series Name', 'Series Code'])
test[test['Country Name'] == 'Afghanistan']['variable'].unique()

# if figure out percentage for each country in Commitments.
for country in com['Country Name'].unique():
    for year in df.columns[4:22]:
        com['pub_pct']
        com[(com['Country Name'] == country) & (com['Series Name'] == 'Commitments, official creditors (COM, current US$)')]
        com[(com['Country Name'] == country) & (com['Series Name'] == 'Commitments, private creditors (COM, current US$)')]
