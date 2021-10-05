from pandas_datareader import data as web

from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
symbols = get_nasdaq_symbols()
print(symbols.loc['IBM'])

US_infl_exp_download = web.DataReader(['T10YIE','T5YIFR'], 'fred')
US_infl_exp_download

from pandas_datareader.famafrench import get_available_datasets
get_available_datasets()


from iexfinance.stocks import Stock
aapl = Stock("AAPL")
# Fund ownership
aapl.get_fund_ownership()
# Institutional ownership
aapl.get_institutional_ownership()

tickers = ['AAPL', 'MSFT', '^GSPC']
start_date = '2010-01-01'
end_date = '2016-12-31'
panel_data = web.DataReader('INPX', start_date, end_date)


US_infl_exp_download

import pandas as pd
import yfinance as yf
msft = yf.Ticker('MSFT')
msft.institutional_holders

!pip install yahoo_fin

import yahoo_fin.stock_info as si
nasdaq_list = si.tickers_nasdaq()
si.get_quote_table("aapl", dict_result=False)
si.get_stats_valuation("aapl")
nasdaq_stats = {}
combined_df = si.get_quote_table(nasdaq_list[0], dict_result = False).T
combined_df.columns = combined_df.iloc[0,:]
combined_df.columns.name = None
combined_df = combined_df.iloc[1:,:]
combined_df = combined_df.reset_index(drop = True)
combined_df.insert(0, 'ticker', '')
combined_df.loc[0,'ticker'] = nasdaq_list[0]
from tqdm import tqdm
temp = si.get_quote_table(nasdaq_list[1], dict_result = False)
temp = temp.T

def get_quote(ticker):
    temp = si.get_quote_table(nasdaq_list[0], dict_result = False).T
    temp.columns = temp.iloc[0,:]
    temp.columns.name = None
    temp = temp.iloc[1:,:]
    temp = temp.reset_index(drop = True)
    temp.insert(0, 'ticker', ticker)
    return temp

import os
from joblib import Parallel, delayed
outlist = [get_quote(ticker) for ticker in tqdm(nasdaq_list)]
# outlist = Parallel(n_jobs=int(os.cpu_count()))(delayed(get_quote)(ticker) for ticker in tqdm(nasdaq_list))
out = pd.concat(outlist)
# [get_quote(ticker) for ticker in tqdm(nasdaq_list[:5])]



mx = si.get_data("GTUSDMX2Y")

amazon_weekly = si.get_data("amzn", start_date="12/04/2009", end_date="12/04/2019", index_as_date = True, interval="1wk")
amazon_weekly.head()
dow_list = si.tickers_dow()
dow_list

import pandas as pd
import yfinance as yf

isin = 'US13063A5G50'
'US13063DMB19'
data = yf.download("AAPL", start="2017-01-01", end="2017-04-30",
                   group_by="ticker")
data



data = yf.download("EI224092", start="2017-01-01", end="2017-04-30",
                   group_by="ticker")
data = yf.download("^IRX", start="2017-01-01", end="2017-04-30",
                   group_by="ticker")
data
data = yf.download("MSFT", start="2017-01-01", end="2017-04-30",
                   group_by="ticker")
msft = yf.Ticker("MSFT")
msft.major_holders
msft.institutional_holders

tbill = yf.Ticker("^IRX")
tbill.major_holders
tbill.institutional_holders

from fredapi import Fred
fred = Fred(api_key='140b492b6059c11a8be1bf39b3fd3455')
data = fred.get_series('SP500')
a = pd.DataFrame(fred.search('concentration').T)
pd.set_option('display.max_rows', 1000)
a.loc[a.index == 'title'].T


fred.search('holders')['title']












from fred import Fred
fr = Fred(api_key = '140b492b6059c11a8be1bf39b3fd3455')
fr = Fred(api_key = '140b492b6059c11a8be1bf39b3fd3455', response_type = 'df')
fr.category.details(1)

params = {'limit':2,
          'tag_names':'trade;goods',
          'order_by':'popularity',
          'sort_order':'desc'}
res = fr.category.series(125, params = params)
for item in res:
    print(item)
print(res)
for record in res:
    print(record)



res = fr.category.details(1)
print(res)

res = fr.category.related(32073)
print(res)

params = {
    'limit':5,
    'tag_names':'trade;goods',
    'order_by':'popularity',
    'sort_order':'desc'
    }

res = fr.category.series(125,params=params)
print(res)
