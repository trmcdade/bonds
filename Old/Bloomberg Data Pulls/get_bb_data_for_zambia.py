import numpy as np
import pandas as pd
import os
from os import listdir
import csv
import xlrd
import xlwt
import pyexcel as pe
# from pyexcel_xlsx import get_data
import datetime
import blpapi
from xbbg import blp
import sys
from tqdm import tqdm

options = blpapi.SessionOptions()
options.setServerHost('localhost')
options.setServerPort(8194)
session = blpapi.Session(options)
session.start()

dir = 'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet'
z = pd.read_csv(dir + '/zambia_fixed_income.csv')
z = pd.Series(z['isin'].unique())
z.to_excel('zambia_isins.xlsx')

isin_list = [f'/isin/{i}' for i in z]
field_list = ['ID_ISIN', 'TICKER', 'Currency', 'CPN_TYP', 'BB_COMPOSITE',
                'MATURITY', 'ISSUE_DT', 'CPN', 'YLD_YTM_BID',
                'YLD_CUR_MID', 'PX_LAST', 'ISSUE_PX', 'REDEMP_VAL']
calc_fields = ['Mty (Yrs)', 'CURR_TYPE', 'HHI']

g.head()



def get_data_for_isin(isin):
    try:
        temp = blp.bdp(tickers = isin, flds = field_list)
        date_list = pd.date_range(temp['issue_dt'][0], temp['maturity'][0], freq = 'MS')
        date_list = [datetime.datetime.strftime(x, '%Y/%m/%d') for x in date_list if x <= pd.to_datetime("today")]

        # g['AMT_OUTSTANDING'] = blp.bdp(tickers="JK424419@CBBT Corp", flds=["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = '2018/01/01')
        aol = [blp.bdp(tickers = isin, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd)['amt_outstanding'][0] for dd in date_list]
        # aol
        temp = pd.concat([temp] * len(aol), ignore_index = True)
        temp['date'] = date_list
        temp['amt_outstanding'] = aol
        temp['country'] = 'ZAMBIA'
        temp['Mty (Yrs)'] = len(pd.date_range(end = temp['maturity'][0], start = temp['issue_dt'][0], freq = 'M')) / 12
        id = isin.split('/')[-1]
        fn = dir + f'/data_{id}.csv'
        temp.to_csv(fn)
        return temp
    except:
        pass

list_of_dfs = [get_data_for_isin(ii) for ii in tqdm(isin_list)]
out = pd.concat(list_of_dfs)
# out = pd.concat([z2, zambia], axis = 0)
ordered_cols = ['country', 'Mty (Yrs)', 'id_isin', 'ticker', 'currency',
                'cpn_typ', 'bb_composite', 'maturity', 'issue_dt',
                'cpn', 'yld_ytm_bid', 'yld_cur_mid', 'px_last', 'issue_px',
                'redemp_val', 'date', 'amt_outstanding']
out = out[ordered_cols].reset_index(drop = True)
out.sort_values(['country', 'id_isin', 'am']).head()

fn_list = [dir + f'/{x}' for x in os.listdir(dir) if x.startswith('data_') and x.endswith('.csv')]
out2 = pd.concat([pd.read_csv(x, index_col = 0) for x in fn_list])
out2 = out2[[x for x in ordered_cols if x in out2.columns]].reset_index(drop = True)
out2.sort_values(['country', 'id_isin', 'date']).head()

# out2['date'] = pd.to_datetime('1900-01-01')

def add_date_col(isin):
    temp = out[out['id_isin'] == isin].copy()
    date_list = pd.date_range(temp['issue_dt'].unique()[0], temp['maturity'].unique()[0], freq = 'MS')
    date_list = [datetime.datetime.strftime(x, '%Y/%m/%d') for x in date_list if x <= pd.to_datetime("today")]
    temp['date'] = date_list
    return temp

out3 = [add_date_col(ii) for ii in out2['id_isin'].unique()]
out3 = pd.concat(out3).reset_index(drop = True)
out3 = out3[ordered_cols]
out3.sort_values(['country', 'id_isin', 'date']).head()

fn = dir + '/zambia_bond_data.csv'
out3.to_csv(fn)
