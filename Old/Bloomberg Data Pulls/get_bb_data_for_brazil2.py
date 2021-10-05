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
28 June 2021
This is almost done. Still do to are:
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
# p = p.drop('X', axis = 1).reset_index(drop = True)
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

# this is done - I can just look at the data files.
## Once I've pulled the security data, I can just pull the info from these files:
fn_list = [dir + f'{x}' for x in os.listdir(dir) if x.startswith('data_') and x.endswith('.csv')]

# fix the amount outstanding data for all the files.
for ff in tqdm(fn_list):
    # ff = [x for x in list_of_files if 'BRSTNCNTC0M0' in x][0]
    # ff = list_of_files[1]
    temp = pd.read_csv(dir + ff, index_col = 0)
    if temp['amt_outstanding'].all() == 0:
        isin = temp['id_isin'].iloc[0]
        id = f'/isin/{isin}'
        print(f'Re-pulling amt out for {id}.')
        date_list = pd.date_range(temp['issue_dt'][0], temp['maturity'][0], freq = 'MS')
        date_list = [datetime.datetime.strftime(x, '%Y/%m/%d') for x in date_list if x <= pd.to_datetime("today")]
        date_list_for_query = [x.replace("/","") for x in date_list]
        aol = [blp.bdp(tickers = id, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd) for dd in date_list_for_query]
        aol = [x['amt_outstanding'][0] if x.shape[0] > 0 else np.NaN for x in aol]
        if not all(aol) == 0:
            temp['amt_outstanding'] = aol
            temp.to_csv(dir + ff)
        else:
            pass
    else:
        pass

if len(fn_list) == len(isin_list):
    out = pd.concat([pd.read_csv(x, index_col = 0) for x in fn_list])
    out = out[[x for x in ordered_cols if x in out.columns]].reset_index(drop = True)
else:
    isins_still_to_do = [x for x in [x.split('.csv')[0].split('data_')[1] for x in fn_list] if x not in [x.split('/')[2] for x in isin_list]]
    list_of_dfs = [get_data_for_isin(ii) for ii in tqdm(isins_still_to_do)]
    out = pd.concat(list_of_dfs)
    fn = dir + f'/{ctry}_security_data.csv'
    out.to_csv(fn)

ordered_cols = ['country', 'Mty (Yrs)', 'id_isin', 'ticker', 'currency',
                'cpn_typ', 'bb_composite', 'maturity', 'issue_dt',
                'cpn', 'yld_ytm_bid', 'yld_cur_mid', 'px_last', 'issue_px',
                'redemp_val', 'date', 'amt_outstanding']
out = out[ordered_cols].reset_index(drop = True)
out.sort_values(['country', 'id_isin', 'date']).head()

## Now, calculate HHI for all dates. Remember to filter by report_date in the
# holdings data before or equal to the Amt_out date.
def calc_hhi(self):
    hhi_fn = f'{ctry}_bond_hhi_data.csv'
    # CHANGE THIS BACK TO NORMAL
    if len([x for x in os.listdir(self.dir) if x == hhi_fn]) < 0:
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
        for ii in tqdm(p['ISIN'].unique()):
            temp = z[z['isin'] == ii]
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
        hhi_df['date'] = pd.PeriodIndex(hhi_df['yyQq'], freq='Q').to_timestamp()
        hhi_df.to_csv(dir + hhi_fn)
    return hhi_df


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

newdates = Parallel(n_jobs = int(os.cpu_count()))(delayed(reformat_date)(dd) for dd in tqdm(p['ref.date']))
p['ref.date'] = newdates

## Take the hhi data and merge it with p.
p['date'] = pd.PeriodIndex(pd.to_datetime(p['ref.date']), freq='Q').to_timestamp()
p = p[p['date'] >= hhi_df['date'].min()]
out_df = p.merge(hhi_df,
                    how = 'left',
                    left_on = ['ISIN', 'date'],
                    right_on = ['isin', 'date']
                    )
# out_df = out_df[out_df['date'] >= '2013-01-01'].reset_index(drop = True)

## I have some empty yyqq and some empty ISINs.
out_df = out_df[~pd.isnull(out_df['ISIN'])]

## WHy is this different? Because there are some ISINs that don't have holdings data.
# len(hhi_df['hhi'].unique())
# len(out_df['hhi'].unique())
# [x for x in hhi_df['isin'].unique() if x not in p['ISIN'].unique()]
# df1 = out_df[['ISIN', 'yyQq', 'hhi', 'date']].drop_duplicates().reset_index(drop = True)
# df2 = hhi_df
# notinlist = [x for x in df1[~df1['hhi'].isin(hhi_df['hhi'])]['ISIN'].unique()]
# hhi_df[hhi_df['hhi'].isin(notinlist)]

out_df.head()
out_df.columns

def get_quarter_diff(start, end):
    t = pd.to_datetime(end).to_period(freq='Q') - pd.to_datetime(start).to_period(freq='Q')
    return(t.n)

out_df['qtm'] = [get_quarter_diff(out_df['ref.date'].iloc[ii], out_df['matur.date'].iloc[ii]) for ii in range(out_df.shape[0])]
out_df.to_csv(dir + f'{ctry}_reg_data.csv')
