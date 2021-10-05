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
datapath = 'C:\\Users\\trmcdade\\OneDrive\\Laptop\\Duke\\RA\\Proposals\\Summer 2019 Proposal\\Data\\Bloomberg'
os.chdir(datapath)
# os.getcwd()

#read csv
df = pd.read_csv('XHDS1_2p3isv4s.csv')
df.rename(columns=dict(zip(df.columns, df.iloc[11])), inplace=True)
df[12:][['Holder Name','% Out']]
