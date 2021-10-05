from yahoo_fin.stock_info import get_data
import pandas as pd

ctry = 'chile'
dir = f'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet/{ctry}/'
pairlist = pd.read_csv(dir + 'pairlist.csv', index_col = 0)

ticker_list = [x for x in pairlist[pd.notnull(pairlist['exp_CUSIP'])]['exp_CUSIP'].unique()][:5]
ticker_list
# ticker_list = ['amzn', 'aapl', 'ba', 'msft']
hd = {}
for ticker in ticker_list:
    hd[ticker] = get_data(ticker)

hd
