import numpy as np
import pandas as pd
import os
from os import listdir
import datetime
import blpapi
from xbbg import blp
import sys
from tqdm import tqdm

'''
18 June 2021
This is almost done. Still do to are:
1. get the amt outstanding for the missing bonds.
2. figure out why all of the hhis aren't transferring
3. take this code and plug it into 'get_bb_data_for_list_of_isins.py'. Ensure
   that the filenames are appropriate because I've done the brazil pull manually.
'''

options = blpapi.SessionOptions()
options.setServerHost('localhost')
options.setServerPort(8194)
session = blpapi.Session(options)
session.start()

ctry = 'brazil'
dir = f'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet/{ctry}/'
z = pd.read_csv(dir + f'{ctry}_fixed_income.csv')
z = pd.Series(z['isin'].unique())
# z.to_excel(f'{ctry}_isins.xlsx')

p = pd.read_csv(dir + 'price_data_for_join_to_pairlist.csv', index_col = 0)
p = p[[x for x in p.columns if not x.endswith('.1')]]

len(p['asset.code'].unique())
isin_list = [f'/isin/{i}' for i in z]
field_list = ['ID_ISIN', 'TICKER', 'Currency', 'CPN_TYP', 'BB_COMPOSITE',
                'MATURITY', 'ISSUE_DT', 'CPN', 'YLD_YTM_BID',
                'YLD_CUR_MID', 'PX_LAST', 'ISSUE_PX', 'REDEMP_VAL']
calc_fields = ['Mty (Yrs)', 'CURR_TYPE', 'HHI']

def get_data_for_isin(isin):
    try:
        temp = blp.bdp(tickers = isin, flds = field_list)
        date_list = pd.date_range(temp['issue_dt'][0], temp['maturity'][0], freq = 'MS')
        date_list = [datetime.datetime.strftime(x, '%Y/%m/%d') for x in date_list if x <= pd.to_datetime("today")]

        aol = [blp.bdp(tickers = isin, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd)['amt_outstanding'][0] for dd in date_list]
        # aol
        temp = pd.concat([temp] * len(aol), ignore_index = True)
        temp['date'] = date_list
        temp['amt_outstanding'] = aol
        temp['country'] = f'{ctry.upper()}'
        temp['Mty (Yrs)'] = len(pd.date_range(end = temp['maturity'][0], start = temp['issue_dt'][0], freq = 'M')) / 12
        id = isin.split('/')[-1]
        fn = dir + f'/data_{id}.csv'
        temp.to_csv(fn)
        return temp
    except:
        pass

list_of_dfs = [get_data_for_isin(ii) for ii in tqdm(isin_list)]
out = pd.concat(list_of_dfs)


# troubleshoot why the amt outstanding pull isn't working.
# it's a quota thing.
out['currency'].hist()
isin = out[out['amt_outstanding'] == 0]['id_isin'].unique()[0]
isin = f'/isin/{isin}'
isin
# isin = isin_list[0]
temp = out[out['id_isin'] == isin]
date_list = pd.date_range(temp['issue_dt'][0], temp['maturity'][0], freq = 'MS')
date_list = [datetime.datetime.strftime(x, '%Y/%m/%d') for x in date_list if x <= pd.to_datetime("today")]
ao = blp.bdp(tickers = isin, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = date_list[0])#['amt_outstanding'][0]
blp.bdp(tickers = isin, flds = ["ISSUE_DT"])
aol = [blp.bdp(tickers = isin, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd)['amt_outstanding'][0] for dd in date_list]

# fix the amount outstanding data for all the files.
list_of_files = [x for x in os.listdir(dir) if x.startswith('data_') and x.endswith('.csv')]
for ff in tqdm(list_of_files):
    # ff = [x for x in list_of_files if 'BRSTNCNTC0M0' in x][0]
    # ff = list_of_files[1]
    temp = pd.read_csv(dir + ff, index_col = 0)
    # temp.head()
    # find the right aol here, once my quota is back, but only if the
    # existing data is all zero
    # temp['amt_outstanding'].all() == 0
    if temp['amt_outstanding'].all() == 0:
        isin = temp['id_isin'].iloc[0]
        id = f'/isin/{isin}'
        print(f'Re-pulling amt out for {id}.')
        date_list = pd.date_range(temp['issue_dt'][0], temp['maturity'][0], freq = 'MS')
        date_list = [datetime.datetime.strftime(x, '%Y/%m/%d') for x in date_list if x <= pd.to_datetime("today")]
        date_list_for_query = [x.replace("/","") for x in date_list]
        # date_list_for_query
        # aol = [blp.bdp(tickers = id, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd)['amt_outstanding'][0] for dd in date_list_for_query]
        aol = [blp.bdp(tickers = id, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd) for dd in date_list_for_query]
        # aol
        aol = [x['amt_outstanding'][0] if x.shape[0] > 0 else np.NaN for x in aol]
        # pd.concat([pd.Series(date_list), pd.Series([x['amt_outstanding'][0] for x in aol])], axis = 1).head()

        # ao = blp.bdp(tickers = '/isin/BRSTNCLF1QR4', flds = "AMT_OUTSTANDING", AMOUNT_OUTSTANDING_AS_OF_DT = '20080601')
        ## test pybbg
        # import pybbg as pybbg
        # bbg = pybbg.Pybbg()
        # ticker = ['/isin/BRSTNCLF1QR4']
        # fld_list = ['AMT_OUTSTANDING']
        # date_list
        # bbg.bdp(ticker, 'AMT_OUTSTANDING', overrides = {'AMOUNT_OUTSTANDING_AS_OF_DT' : '20080601'})
        # bbg.bdp(ticker, 'ISSUE_DT')
        # bbg.bdh(ticker, fld_list, '20080601', end_date = '20081001', periodselection = 'MONTHLY')
        # ao = bbg.bdp('/isin/BRSTNCLF1QR4', flds = "AMT_OUTSTANDING", AMOUNT_OUTSTANDING_AS_OF_DT = '2008/06/01')
        # bbg.bdp('PGB 1.95 06/15/2029 Govt', ['MATURITY', 'COUPON', 'ISSUE_DT', 'YLD_YTM_MID'])

        # aol = pd.read_csv(dir + list_of_files[0], index_col = 0)['amt_outstanding']
        if not all(aol) == 0:
            temp['amt_outstanding'] = aol
            temp.to_csv(dir + ff)
        else:
            pass
    else:
        pass

fn = dir + f'/{ctry}_security_data.csv'
out.to_csv(fn)
# out = pd.concat([z2, zambia], axis = 0)
ordered_cols = ['country', 'Mty (Yrs)', 'id_isin', 'ticker', 'currency',
                'cpn_typ', 'bb_composite', 'maturity', 'issue_dt',
                'cpn', 'yld_ytm_bid', 'yld_cur_mid', 'px_last', 'issue_px',
                'redemp_val', 'date', 'amt_outstanding']
out = out[ordered_cols].reset_index(drop = True)
out.sort_values(['country', 'id_isin', 'am']).head()

## Once I've pulled the security data, I can just pull the info from these files:
fn_list = [dir + f'{x}' for x in os.listdir(dir) if x.startswith('data_') and x.endswith('.csv')]
out2 = pd.concat([pd.read_csv(x, index_col = 0) for x in fn_list])
out2 = out2[[x for x in ordered_cols if x in out2.columns]].reset_index(drop = True)
out2.sort_values(['country', 'id_isin', 'date']).head()
out = out2

## Now, calculate HHI for all dates. Remember to filter by report_date in the
# holdings data before or equal to the Amt_out date.
def calc_hhi(self):
    hhi_fn = f'{ctry}_bond_hhi_data_partial.csv'
    if len([x for x in os.listdir(self.dir) if x == hhi_fn]) > 0:
        print('Data already exists. Moving on.')
        hhi_df = pd.read_csv(self.dir + hhi_fn, index_col = 0)
    else:
        print('Calculating the HHI for each security over time.')
        z = pd.read_csv(dir + f'{ctry}_fixed_income.csv')
        z = z[pd.notnull(z['report_date'])].copy().reset_index(drop = True)
        z['report_date'] = [datetime.datetime.strftime(pd.to_datetime(x).replace(day=1), '%Y/%m/%d') for x in tqdm(z['report_date'])]

        month_to_quarter_dict = {'1':'1', '2':'1', '3':'1',
                                    '4':'2', '5':'2', '6':'2',
                                    '7':'3', '8':'3', '9':'3',
                                    '10':'4', '11':'4', '12':'4'}
        month_to_quarter_dict2 = {'01':'1', '02':'1', '03':'1',
                                    '04':'2', '05':'2', '06':'2',
                                    '07':'3', '08':'3', '09':'3',
                                    '10':'4', '11':'4', '12':'4'}

        z['Q'] = [month_to_quarter_dict2[x.split('/')[1]] for x in z['report_date']]
        z['year'] = [x.split('/')[0] for x in z['report_date']]
        z['yyQq'] = [z['year'].iloc[x] + '-Q' + z['Q'].iloc[x] for x in range(z.shape[0])]
        z = z[['isin', 'factset_entity_id', 'entity_proper_name', 'report_date', 'reported_holding', 'year', 'Q', 'yyQq']].copy()
        z = z.drop(['report_date'], axis = 1)

        # z[z['entity_proper_name'] == 'Voya Balanced Portfolio'].sort_values(['isin', 'report_date'])
        # test = z.drop(['report_date'], axis = 1).groupby(['isin', 'factset_entity_id', 'entity_proper_name', 'year', 'Q']).sum().reset_index()
        # test[(test['entity_proper_name'] == 'Voya Balanced Portfolio') & (test['isin'] == 'US105756BF62')].sort_values(['year', 'Q'])

        # out.head()
        out['Q'] = [month_to_quarter_dict2[x.split('/')[1]] for x in out['date']]
        out['year'] = [x.split('/')[0] for x in out['date']]
        out['yyQq'] = [out['year'].iloc[x] + '-Q' + out['Q'].iloc[x] for x in range(out.shape[0])]
        out = out.drop(['date'], axis = 1)

        list_of_dicts = []
        for ii in tqdm(z['isin'].unique()):
            # ii = z['isin'].unique()[1]
            temp = z[z['isin'] == ii]
            # temp.head()
            # out.head()
            # TODO: get rid of this if clause and unindent once i fix the
            # aol thing. It's going to be at the quarterly level since
            # it looks like that's what the holdings data is at.
            if not out[out['id_isin'] == ii]['amt_outstanding'].all() == 0:
                # print('theyre not zero go ahead')

                # combined = temp.merge(out[['id_isin', 'date', 'amt_outstanding', 'year', 'Q', 'yyQq']],
                combined = temp.merge(out[['id_isin', 'amt_outstanding', 'year', 'Q', 'yyQq']],
                                        how = 'left',
                                        # left_on = ['isin', 'report_date'],
                                        # right_on = ['id_isin', 'date']
                                        left_on = ['isin', 'year', 'Q', 'yyQq'],
                                        right_on = ['id_isin', 'year', 'Q', 'yyQq']
                                        )
                # out.shape
                out[(out['id_isin'] == 'BRSTNCNTF147') & (out['yyQq'] == '2016Q4')]
                # temp.shape
                temp[(temp['isin'] == 'BRSTNCNTF147') & (temp['yyQq'] == '2016Q4')]
                # combined.shape
                combined.head()
                # for dd in combined['date'].unique():
                for dd in combined['yyQq'].unique():
                    # dd = combined['date'].unique()[0]
                    # dd = combined['yyQq'].unique()[0]
                    # di_temp = combined[combined['date'] == dd].copy()
                    di_temp = combined[combined['yyQq'] == dd].copy()
                    di_temp['pct_os'] = di_temp['reported_holding'].divide(di_temp['amt_outstanding'])
                    di_temp['pct_os_sq'] = di_temp['pct_os'] ** 2
                    hhi = di_temp['pct_os_sq'].sum()
                    hhi_temp_out = dict()
                    hhi_temp_out['isin'] = ii
                    # hhi_temp_out['date'] = dd
                    hhi_temp_out['yyQq'] = dd
                    hhi_temp_out['hhi'] = hhi
                    list_of_dicts.append(hhi_temp_out)
        hhi_df = pd.DataFrame(list_of_dicts)
        hhi_df.to_csv(dir + hhi_fn)
    return hhi_df

hhi_df = pd.DataFrame(list_of_dicts)
hhi_df[hhi_df['isin'] == hhi_df['isin'].unique()[0]]
hhi_df['date'] = pd.PeriodIndex(hhi_df['yyQq'], freq='Q').to_timestamp()
hhi_df.head()

import seaborn as sns
import matplotlib.pyplot as plt
for ii in hhi_df['isin'].unique():
    plt.figure()
    sns.lineplot(data = hhi_df[hhi_df['isin'] == ii], x = 'date', y = 'hhi')
    plt.title(f'{ii}')


# merge hhi data with the rest of the security data
# TODO: fix date format. make sure they're all the beginning of the month.
# merge on isin and date.

def reformat_date(dd):
    return datetime.datetime.strptime(dd, '%Y-%m-%d').strftime('%Y/%m/%d')

import os
from joblib import Parallel, delayed
newdates = Parallel(n_jobs = int(os.cpu_count()))(delayed(reformat_date)(dd) for dd in tqdm(p['ref.date']))
p['ref.date'] = newdates

## Take the hhi data and merge it with p.
sorted(p.columns)
p['ref.date'].head()
p['date'] = pd.PeriodIndex(pd.to_datetime(p['ref.date']), freq='Q').to_timestamp()
hhi_df.head()

out_df = p.merge(hhi_df,
                    how = 'left',
                    left_on = ['ISIN', 'date'],
                    right_on = ['isin', 'date']
                    )
out_df = out_df[out_df['date'] >= '2013-01-01'].reset_index(drop = True)
out_df.head()

out_df.to_csv(dir + f'{ctry}_reg_data.csv')

## WHy is this different?
len(hhi_df['hhi'].unique())
len(out_df['hhi'].unique())


out2.to_csv(fn)
