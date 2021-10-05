import os
from os import listdir
import pandas as pd
import numpy as np
import matplotlib
from matplotlib import pyplot as plt
import matplotlib.patches as mpatches
from matplotlib import dates as mdates
from matplotlib.lines import Line2D
import datetime
from pandas.plotting import autocorrelation_plot, lag_plot
import seaborn as sns
from tqdm import tqdm

from joblib import Parallel, delayed
import blpapi
from xbbg import blp

## regroup. i have two choices: brazil or CA munis.
# if i want to drop the secondary-primary market connection because there is
# some literature on this, then I need to:
# 1. get security information about the bonds I am interested in.
# 2. calculate the hhi for those bonds.
# 3. calculate fragility?
# 4. calculate price volatility?
# 5. calculate the volatility risk premium? going to be complicated to model.
# but schumacher says that the yields converge to the short term risk free rate
# near maturity. So then do I need the price data at all? Or just the yield curve
# on a daily basis? Yes, I do need it because my variable is the YTM of the
# security.
## i think that the best path

options = blpapi.SessionOptions()
options.setServerHost('localhost')
options.setServerPort(8194)
session = blpapi.Session(options)
session.start()

ctry = 'brazil'
# ctry = 'chile'
# dir = "C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/Bloomberg/"
dir = f'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet/{ctry}/'

# while I have no access to bbg:
# a = pd.read_csv('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/Output_Data/Issues/bond_issuances_py.csv', index_col = 0)
# a = a[(a['Country (Full Name)'] == ctry.upper()) & (a['Maturity (Months)'] >= 0)].copy()

# if I have access to bbg:
a = pd.read_csv(dir + f'{ctry.lower()}_reg_data.csv', index_col = 0)
a = a[(a['hhi'] <= 100) & (a['hhi'] != 0)].copy().reset_index(drop = True)
a['bb_composite'] = a['bb_composite'].fillna('NR')
a['yld_cur_mid'] = [x for x in pd.to_numeric(a['yld_cur_mid'], errors = 'coerce')]
a['diff'] = a['yld_ytm_bid'].subtract(a['cpn'])
a['issue_px'] = a['issue_px'].replace('#N/A Field Not Applicable', np.nan)

bond_data_fn = 'C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/Output_Data/Issues/bond_issuances_py.csv'
# bond_data_fn = 'C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/bond_issuances_py.csv'
j = pd.read_csv(bond_data_fn, index_col = 0)
# j = j[j['Maturity Date'] >= '2013-01-01'].copy().reset_index(drop = True)
# j = j[j['Maturity Date'] >= '2013-01-01'][['Country (Full Name)', 'ISIN', 'Bloomberg ID']]
# j.to_csv('C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/bond_issuances_py_for_sql_upload.csv')
j = j[(j['Country (Full Name)'] == ctry.upper()) & (j['Maturity (Months)'] >= 0)].copy()
j = j.sort_values(by = ['Maturity (Months)', 'Issue Date']).reset_index(drop = True)
j['Maturity Date'] = pd.to_datetime(j['Maturity Date'])
j['Issue Date'] = pd.to_datetime(j['Issue Date'])

## why are only a few of the holdings isins in the bloomberg data and
## only a few of the bloomberg isins in the holdings data?
len(j['ISIN'].unique())
len(a['isin'].unique())
included_isins = [x for x in a['isin'].unique() if x in j['ISIN'].unique()]
len(included_isins)
# sorted(a[a['isin'].isin(included_isins)]['issue_dt'].unique())
## excluded ones are just after a certain time period.
excluded_isins = [x for x in a['isin'].unique() if x not in j['ISIN'].unique()]
len(excluded_isins)
# sorted(a[a['isin'].isin(excluded_isins)]['issue_dt'].unique())

## now find the next one issued after the expiration of each ISIN in a.
## Use tapped status to figure out if it was reissued and when relative
## to the original maturity date and what the amount was.
## Incorporate all this into the POC graph
# j[(j['Was Bond Tapped'].isin(['Y'])) & (j['Issue Date'] >= pd.Timestamp(2013,1,1)) & (j['Maturity (Yrs)'] <= 1)].sort_values(['ISIN']).iloc[:30]

# consider some ISIN: ARARGE3200D7.
# find all the bonds issued near its maturity date that have the same profile as it:
# curr, mty,
# u = j.sample(10)
# expiring = j[j['ISIN'] == 'ARARGE3200D7']
# for isin in u['ISIN'].unique():
#     expiring = j[j['ISIN'] == isin]
#     curr = expiring['Curr'].unique()[0]
#     mty = round(expiring['Maturity (Yrs)'].mean())
#     mty_date = pd.Timestamp(expiring['Maturity Date'].unique()[0])
#
#     y = j[(j['Issue Date'] - mty_date < pd.Timedelta(90, 'D')) &
#         (j['Issue Date'] - mty_date > pd.Timedelta(-90, 'D')) &
#         (j['Curr'] == curr) &
#         (round(j['Maturity (Yrs)']) == mty)]
#     print(y.shape)


def num_months(end_date, start_date):
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    return (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)

def get_matches(expiring, next):
    mty_months = expiring['Maturity (Months)']
    # bbgid = next['Bloomberg ID']
    # bbgid = next['index']
    isin = next['ISIN']
    cusip = next['Bloomberg ID']
    mths_to_next_issue = num_months(next['Issue Date'], expiring['Maturity Date'])
    return (mty_months, isin, cusip, mths_to_next_issue)

def whittle_down_possible_matches(expiring):
    # find possible matches

    temp = j[(j['Issue Date'] - expiring['Maturity Date'] < pd.Timedelta(90, 'D')) &
                (j['Issue Date'] - expiring['Maturity Date'] > pd.Timedelta(-90, 'D')) & # within 90 days of maturity
                (j['Curr'] == expiring['Curr']) & # same currency
                (round(j['Maturity (Yrs)']) == round(expiring['Maturity (Yrs)']))] # same ish maturity length.

    # temp = j[(j['Issue Date'] >= expiring['Maturity Date']) & (j['Maturity (Months)'] == expiring['Maturity (Months)']) & (j['Curr'] == expiring['Curr'])]

    if temp.shape[0] > 0:
        temp = temp[temp['Issue Date'] == min(temp['Issue Date'])]
        # matches = [get_matches(expiring, temp.iloc[row]) for row in range(temp.shape[0])]
        outlist = [get_matches(expiring, temp.iloc[y]) for y in range(temp.shape[0])]
        # outlist = [(expiring['Maturity (Months)'], temp.iloc[y]['Bloomberg ID'], num_months(temp.iloc[y]['Issue Date'], expiring['Maturity Date'])) for y in range(temp.shape[0])]
        outlist = pd.DataFrame(outlist, columns = ['Mat Months', 'ISIN', 'CUSIP', 'Num Months']).sort_values('Num Months', ascending = True)
        mths_to_next_issue = min(outlist['Num Months'])
        mty_months = expiring['Maturity (Months)']
        newisin = outlist['ISIN'].iloc[0]
        newcusip = outlist['CUSIP'].iloc[0]
    else:
        mths_to_next_issue = np.NaN
        mty_months = expiring['Maturity (Months)']
        newisin = np.NaN
        newcusip = np.NaN
    curr = expiring['Curr']
    expisin = expiring['ISIN']
    expcusip = expiring['Bloomberg ID']
    return mty_months, expisin, expcusip, newisin, newcusip, mths_to_next_issue, curr

# pairlist = [whittle_down_possible_matches(j.iloc[row]) for row in tqdm(range(j.shape[0]))]
# pairlist = pd.DataFrame(pairlist, columns = ['mty_months', 'exp_ISIN', 'exp_CUSIP', 'reissue_ISIN', 'reissue_CUSIP', 'months_to_reissue', 'curr'])
# pairlist.to_csv(dir + 'pairlist.csv')
pairlist = pd.read_csv(dir + 'pairlist.csv', index_col = 0)
## the above has a bunch of non-null entries that are mapped to a bunch of non-null reissues.

## now, add the coupon at issuance of the new isin to the expiring isin.
## a has all the info about the expiring bonds.
# add a column about time from now until maturity.
a['days_to_mty'] = [(pd.to_datetime(a['maturity'].iloc[x]) - datetime.datetime.today()).days for x in range(a.shape[0])]
# a['days_to_mty'] = [(pd.to_datetime(a['Maturity Date'].iloc[x]) - datetime.datetime.today()).days for x in range(a.shape[0])]
# indep var will be an interaction between time til maturity and px_last.
## TODO: tomorrow, start here. figure out why there's only one option here
## for cusips and no isins. then take this down and merge thte reissue data
# with the other bond data.

reissue_isins = [x for x in pairlist['reissue_ISIN'].unique() if not pd.isnull(x)]
reissue_cusips = [x for x in pairlist['reissue_CUSIP'].unique() if not pd.isnull(x)]
# the bloombeg data i pulled about the securities that are reissues for others.
reissues_info = j[(j['ISIN'].isin(reissue_isins)) | (j['Bloomberg ID'].isin(reissue_cusips))].copy().reset_index(drop = True)

# this is saying that there is info for two reissue isins and one reissue cusip
# in the holdings data.

# sorted(j.columns)
# ordered_cols = ['country', 'Mty (Yrs)', 'id_isin', 'id_cusip', 'ticker', 'currency',
#                     'cpn_typ', 'bb_composite', 'maturity', 'issue_dt',
#                     'cpn', 'yld_ytm_bid', 'yld_cur_mid', 'px_last', 'issue_px',
#                     'redemp_val', 'date', 'amt_issued']

isin_list = [f'/isin/{x}' for x in j['ISIN'].unique() if not pd.isnull(x)]
cusip_list = [f'/cusip/{x}' for x in j['Bloomberg ID'].unique() if not pd.isnull(x)]

field_list = ['Country', 'Mty (Yrs)', 'ISSUE_DT', 'MATURITY',
                'ID_ISIN', 'ID_CUSIP', 'TICKER',
                 'Currency', 'BB_COMPOSITE',
                'ISSUE_PX', 'REDEMP_VAL', 'AMT_ISSUED',
                'CPN', 'CPN_TYP',
                'YLD_YTM_BID', 'YLD_CUR_MID', 'PX_LAST']

            # alternate arrangement: avg of price over the last month before maturity.
            # maturity_price = blp.bdh(tickers = id, flds = 'PX_OPEN', start_date = issue_date_str, end_date = one_mo_after_str, Per = 'W', Fill='P', Days='A').iloc[0,0]


id = isin_list[0]
id = cusip_list[0]
type = id.split('/')[1]
id
blp.bdp(tickers = id, flds = 'ISSUE_DT')
blp.bdp(tickers='NVDA US Equity', flds=['Security_Name','GICS_Sector_Name'])

temp

listoftickers = cusip_list[:5]
listoftickers
blp.bdp(tickers = listoftickers[0], flds = 'ISSUE_DT')

r = blp.bdp(tickers = listoftickers, flds = field_list[4:])
r

def get_easy_data_for_securities(id_list):
    '''
    Find relevant attributes of multiple securities at once.
    '''
    # id_list = listoftickers
    security_list = [x.split('/')[-1] for x in id_list]
    type = list(set([x.split('/')[1] for x in id_list]))[0]
    id_dict = {'cusip':'Bloomberg ID', 'isin':'ISIN'}

    try:
        # find the data for the list of fields we care about.
        temp = pd.DataFrame(columns = [x.lower() for x in field_list[4:]])
        try:
            r = blp.bdp(tickers = id_list, flds = field_list[4:])
            temp = pd.concat([temp, r], axis = 0)
        except:
            for fld in field_list[4:]:
                temp[fld.lower()] = np.NaN

        temp['country'] = ctry.upper()

        temp['issue_dt'] = [j[j[id_dict[type]] == ss]['Issue Date'].iloc[0] for ss in security_list]
        temp['maturity'] = [j[j[id_dict[type]] == ss]['Maturity Date'].iloc[0] for ss in security_list]

        # if type == 'cusip':
        #     temp['issue_dt'] = [j[j['Bloomberg ID'] == ss]['Issue Date'].iloc[0] for ss in security_list]
        #     temp['maturity'] = [j[j['Bloomberg ID'] == ss]['Maturity Date'].iloc[0] for ss in security_list]
        # elif type == 'isin':
        #     temp['issue_dt'] = [j[j['ISIN'] == ss]['Issue Date'].iloc[0] for ss in security_list]
        #     temp['maturity'] = [j[j['ISIN'] == ss]['Maturity Date'].iloc[0] for ss in security_list]

        temp['Mty (Yrs)'] = [len(pd.date_range(end = temp['maturity'].iloc[i], start = temp['issue_dt'].iloc[i], freq = 'M')) / 12 for i in range(temp.shape[0])]

        if temp[pd.notnull(temp[f'id_{type}'])].shape[0] < len(security_list):
            print('Attention: there are null ids. You could have hit the Bloomberg data quota.')

        # save the file.
        # fn = self.dir + f'data_{id}.csv'
        # temp.to_csv(fn)
        return temp
    except:
        print('Retrieval failed. Moving on.')
        pass

listoftickers
get_easy_data_for_securities(listoftickers)
temp

def find_date(id, type_of_date):
    '''
    returns the issue or maturity date for a given security
    '''
    # id = listoftickers[0]
    ss = id.split('/')[-1]
    type = id.split('/')[1]
    col_dict = {'issue_dt':'Issue Date', 'maturity':'Maturity Date'}
    # typeofdate = 'issue_dt'

    if type_of_date in ['issue_dt', 'maturity']:
        if type == 'cusip':
            out = j[j['Bloomberg ID'] == ss][col_dict[typeofdate]].iloc[0]
        elif type == 'isin':
            out = j[j['ISIN'] == ss][col_dict[typeofdate]].iloc[0]

    if not isinstance(out, str):
        one_mo_before = pd.to_datetime(out) - pd.Timedelta(30, 'D')
        one_mo_before = datetime.datetime.strftime(one_mo_before, '%Y/%m/%d')
        out = datetime.datetime.strftime(out, '%Y/%m/%d')
    return out, one_mo_before

find_date(listoftickers[0], 'issue_dt')

# TODO: start here after lunch.
def get_tricky_data_for_security(id):
    '''
    Find relevant attributes of a given security.
    '''

    # id = isin_list[0]
    # id
    # id = isin.split('/')[-1]
    type = id.split('/')[1]

    try:
        # find the data for the list of fields we care about.

        if type == 'cusip':
            cusip = id.split('/')[-1]
            issue_date = j[j['Bloomberg ID'] == cusip]['Issue Date'].iloc[0]
            mat_date = j[j['Bloomberg ID'] == cusip]['Maturity Date'].iloc[0]

        elif type == 'isin':
            isin = id.split('/')[-1]
            issue_date = j[j['ISIN'] == isin]['Issue Date'].iloc[0]
            mat_date = j[j['ISIN'] == isin]['Maturity Date'].iloc[0]

        issue_date_str = datetime.datetime.strftime(issue_date, '%Y/%m/%d')
        one_mo_after = pd.to_datetime(issue_date_str) + pd.Timedelta(7, 'D')
        one_mo_after_str = datetime.datetime.strftime(one_mo_after, '%Y/%m/%d')

        mat_date_str = datetime.datetime.strftime(mat_date, '%Y/%m/%d')
        one_mo_before = pd.to_datetime(mat_date_str) - pd.Timedelta(7, 'D')
        one_mo_before_str = datetime.datetime.strftime(one_mo_before, '%Y/%m/%d')

        tricky_field_list = ['ISSUE_PX', 'YLD_YTM_BID', 'YLD_CUR_MID', 'maturity_price', 'PX_LAST']
        temp = {k.lower() : np.NaN for k in tricky_field_list}

        if pd.isnull(temp['issue_px']):
            try:
                temp['issue_px'] = blp.bdh(tickers = id, flds = 'PX_OPEN', start_date = issue_date_str, end_date = issue_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
            except:
                try:
                    temp['issue_px'] = blp.bdh(tickers = id, flds = 'PX_LAST', start_date = issue_date_str, end_date = issue_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
                except:
                    temp['issue_px'] = np.NaN

        if pd.to_datetime(temp['maturity']) >= datetime.datetime.today():
            temp['yld_ytm_bid'] = blp.bdp(tickers = id, flds = 'YLD_YTM_BID').iloc[0,0]
            temp['yld_cur_mid'] = blp.bdp(tickers = id, flds = 'YLD_CUR_MID').iloc[0,0]
            temp['px_last'] = blp.bdp(tickers = id, flds = 'PX_LAST').iloc[0,0]
        else:
            try:
                temp['yld_ytm_bid'] = blp.bdh(tickers = id, flds = 'YLD_YTM_BID', start_date = mat_date_str, end_date = mat_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
            except:
                temp['yld_ytm_bid'] = np.NaN
            try:
                temp['yld_cur_mid'] = blp.bdh(tickers = id, flds = 'YLD_CUR_MID', start_date = mat_date_str, end_date = mat_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
            except:
                temp['yld_cur_mid'] = np.NaN

            try:
                temp['maturity_price'] = blp.bdh(tickers = id, flds = 'PX_OPEN', start_date = mat_date_str, end_date = mat_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
            except:
                try:
                    temp['maturity_price'] = blp.bdh(tickers = id, flds = 'PX_LAST', start_date = mat_date_str, end_date = mat_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
                except:
                    temp['maturity_price'] = np.NaN

            # alternate arrangement: avg of price over the last month before maturity.
            # maturity_price = blp.bdh(tickers = id, flds = 'PX_OPEN', start_date = issue_date_str, end_date = one_mo_after_str, Per = 'W', Fill='P', Days='A').iloc[0,0]

            # print(temp)
        # save the file.
        # fn = self.dir + f'data_{id}.csv'
        # temp.to_csv(fn)
        return temp
    except:
        print('Retrieval failed. Moving on.')
        pass



def get_data_for_security_old(id):
    '''
    Find relevant attributes of a given security.
    '''
    # id = isin_list[0]
    # id
    # id = isin.split('/')[-1]
    type = id.split('/')[1]

    # fn = self.dir + f'data_{id}.csv'

    try:
        # find the data for the list of fields we care about.
        # id = cusip_list[0]
        # type = 'cusip'
        # print(id)
        # print(type)
        # temp = blp.bdp(tickers = id, flds = field_list)

        if type == 'cusip':
            cusip = id.split('/')[-1]
            issue_date = j[j['Bloomberg ID'] == cusip]['Issue Date'].iloc[0]
            mat_date = j[j['Bloomberg ID'] == cusip]['Maturity Date'].iloc[0]

        elif type == 'isin':
            isin = id.split('/')[-1]
            issue_date = j[j['ISIN'] == isin]['Issue Date'].iloc[0]
            mat_date = j[j['ISIN'] == isin]['Maturity Date'].iloc[0]

        issue_date_str = datetime.datetime.strftime(issue_date, '%Y/%m/%d')
        one_mo_after = pd.to_datetime(issue_date_str) + pd.Timedelta(7, 'D')
        one_mo_after_str = datetime.datetime.strftime(one_mo_after, '%Y/%m/%d')

        mat_date_str = datetime.datetime.strftime(mat_date, '%Y/%m/%d')
        one_mo_before = pd.to_datetime(mat_date_str) - pd.Timedelta(7, 'D')
        one_mo_before_str = datetime.datetime.strftime(one_mo_before, '%Y/%m/%d')

        temp = {k.lower() : np.NaN for k in field_list}
        temp['country'] = ctry.upper()
        temp['issue_dt'] = issue_date
        temp['maturity'] = mat_date
        temp['Mty (Yrs)'] = len(pd.date_range(end = temp['maturity'], start = temp['issue_dt'], freq = 'M')) / 12

        tricky_field_list = ['ISSUE_PX', 'YLD_YTM_BID', 'YLD_CUR_MID', 'maturity_price', 'PX_LAST']

        if pd.isnull(temp['issue_px']):
            try:
                temp['issue_px'] = blp.bdh(tickers = id, flds = 'PX_OPEN', start_date = issue_date_str, end_date = issue_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
            except:
                try:
                    temp['issue_px'] = blp.bdh(tickers = id, flds = 'PX_LAST', start_date = issue_date_str, end_date = issue_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
                except:
                    temp['issue_px'] = np.NaN


        if pd.to_datetime(temp['maturity']) >= datetime.datetime.today():
            temp['yld_ytm_bid'] = blp.bdp(tickers = id, flds = 'YLD_YTM_BID').iloc[0,0]
            temp['yld_cur_mid'] = blp.bdp(tickers = id, flds = 'YLD_CUR_MID').iloc[0,0]
            temp['px_last'] = blp.bdp(tickers = id, flds = 'PX_LAST').iloc[0,0]
        else:
            try:
                temp['yld_ytm_bid'] = blp.bdh(tickers = id, flds = 'YLD_YTM_BID', start_date = mat_date_str, end_date = mat_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
            except:
                temp['yld_ytm_bid'] = np.NaN
            try:
                temp['yld_cur_mid'] = blp.bdh(tickers = id, flds = 'YLD_CUR_MID', start_date = mat_date_str, end_date = mat_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
            except:
                temp['yld_cur_mid'] = np.NaN

            try:
                temp['maturity_price'] = blp.bdh(tickers = id, flds = 'PX_OPEN', start_date = mat_date_str, end_date = mat_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
            except:
                try:
                    temp['maturity_price'] = blp.bdh(tickers = id, flds = 'PX_LAST', start_date = mat_date_str, end_date = mat_date_str, Per = 'D', Fill='P', Days='A').iloc[0,0]
                except:
                    temp['maturity_price'] = np.NaN

            # alternate arrangement: avg of price over the last month before maturity.
            # maturity_price = blp.bdh(tickers = id, flds = 'PX_OPEN', start_date = issue_date_str, end_date = one_mo_after_str, Per = 'W', Fill='P', Days='A').iloc[0,0]

            # print(temp)
        # save the file.
        # fn = self.dir + f'data_{id}.csv'
        # temp.to_csv(fn)
        return temp
    except:
        print('Retrieval failed. Moving on.')
        pass

## Get all the info for the isins and cusips.
# get_data_for_security(isin_list[0], 'isin')
# isin_dfs = [get_data_for_security(ii) for ii in tqdm(isin_list)] # USE THIS ONE
# isin_dfs = Parallel(n_jobs = int(os.cpu_count()))(delayed(get_data_for_security)(ii) for ii in tqdm(isin_list))
# isin_df = pd.DataFrame(isin_dfs)
# isin_df = pd.DataFrame([x for x in isin_dfs if x is not None])
# isin_df.to_csv(dir + 'isin_df.csv')
isin_df = pd.read_csv(dir + 'isin_df.csv', index_col = 0)

## before I do the next step: why are there more than one value for the bb_composite
# column? Are they getting concatenated worng?
# cusip_df = pd.DataFrame(cusip_dfs)
# cusip_dfs = [get_data_for_security(cc) for cc in tqdm(cusip_list)] # USE THIS ONE
# cusip_dfs = Parallel(n_jobs = int(os.cpu_count()))(delayed(get_data_for_security)(cc) for cc in tqdm(cusip_list))
# cusip_df = pd.DataFrame([x for x in cusip_dfs if x is not None])
# cusip_df
# cusip_df.to_csv(dir + 'cusip_df.csv')
cusip_df = pd.read_csv(dir + 'cusip_df.csv', index_col = 0)
# cusip_df = cusip_df[pd.notnull(cusip_df['id_cusip'])]
#############3

# lots of nans. Why? Looks like an api thing to me.
cusip_df[pd.isnull(cusip_df['ticker'])].sample(5)
i = 3458
cusip_list[i]
get_data_for_security(cusip_list[i])
blp.bdp(tickers = cusip_list[i], flds = 'ISSUE_DT')
# Looks like the process stopped halfway or something because
# a ton of the records are nan. re-do them.
# get the indices of the null values.
indexes = [i for i,x in enumerate(cusip_dfs) if x is not None and pd.isnull(x['id_cusip'])]
cusips_still_to_do = list(map(cusip_list.__getitem__, indexes))
cusip2_dfs = Parallel(n_jobs = int(os.cpu_count()))(delayed(get_data_for_security)(cc) for cc in tqdm(cusips_still_to_do))
get_data_for_security(cusip_list[0])
blp.bdp(tickers = cusips_still_to_do[0], flds = 'ISSUE_DT')
cusip2_df = pd.DataFrame([x for x in cusip2_dfs if x is not None])
## now append them to the first df.

# check and see if any of the fields have wrong values.
[(ff, cusip_df[ff].unique()) for ff in cusip_df.columns]


bond_info = pd.concat([isin_df, cusip_df], axis = 0).drop_duplicates().reset_index(drop = True)

## while I have no bloomberg.
sorted([x for x in a.columns])
bond_info = a[['ISIN', 'Bloomberg ID', 'Issue Date', 'Cpn', 'Curr',
                'Inflation-Linked Note', 'Maturity (Months)', 'Maturity (Yrs)',
                'Amount Issued', 'Price at Issue', 'Total Bond Issued Amount',
                'days_to_mty']].copy().reset_index(drop = True)
bond_info = bond_info.rename(columns = {'Bloomberg ID' : 'CUSIP'})
reissues_info = bond_info.add_prefix('reissue_')
## end


bond_info = bond_info.drop('mty (yrs)', axis = 1)
cols_relevant_for_reissue = ['id_isin', 'id_cusip',
                                'Mty (Yrs)', 'ticker', #'currency',
                                # 'bb_composite',
                                'redemp_val', 'amt_issued',
                                'cpn', 'cpn_typ',
                                'yld_ytm_bid', 'yld_cur_mid',
                                'issue_px', 'px_last', 'maturity_price']

reissues_info = bond_info[cols_relevant_for_reissue].reset_index(drop = True)
reissues_info = reissues_info.add_prefix('reissue_')
reissues_info = reissues_info.rename(columns = {'reissue_id_isin' : 'reissue_ISIN', 'reissue_id_cusip' : 'reissue_CUSIP'})

issues_info = bond_info #.reset_index(drop = True)
issues_info = issues_info.rename(columns = {'id_isin' : 'ISIN', 'id_cusip' : 'CUSIP'})

reissues_info.shape
issues_info.shape

## ok, now that everything is straight, I now need to join this data about reissues
# to the expiring securities. Then I will be able to use the characteristics
# of the expiring securities to predict the characteristics of the reissuing
# securities.
# the next step is to take what I just did -- finding all those prices --
# and do it for every security in j. That way I have the price at issue and
# the price at maturity for every bond, and all I have to do is come up with
# pairlist as a mapping table, make a copy of the bond info df and relabel one
# of them with the prefix 'reissue_'. Then I can join both the original bond
# info df back to pairlist on te regular ids and join the reissue df to
# pairlist on the reissue ids.
# I need to rerun this with the new isin_list and the new cusip_list.

map = pairlist[['exp_ISIN', 'exp_CUSIP', 'reissue_ISIN', 'reissue_CUSIP', 'months_to_reissue']].drop_duplicates().reset_index(drop = True)
map.shape
map['exp_ISIN'].unique()[1]
map[map['exp_ISIN'] == map['exp_ISIN'].unique()[1]]

issues_isins = map.merge(issues_info[pd.notnull(issues_info['ISIN'])].reset_index(drop = True),
                                                            how = 'left',
                                                            left_on = ['exp_ISIN'],
                                                            right_on = ['ISIN'])
issues_isins.shape
issues_cusips = map.merge(issues_info[pd.notnull(issues_info['CUSIP'])].reset_index(drop = True),
                                                            how = 'left',
                                                            left_on = ['exp_CUSIP'],
                                                            right_on = ['CUSIP'])
issues = issues_isins
for col in issues.columns:
    issues[col].fillna(issues_cusips[col], inplace=True)
issues.shape

## join the reissues data together
reissues_isins = map.merge(reissues_info[pd.notnull(reissues_info['reissue_ISIN'])].drop('reissue_CUSIP', axis = 1).reset_index(drop = True),
                                                            how = 'left',
                                                            left_on = ['reissue_ISIN'],
                                                            right_on = ['reissue_ISIN'])
reissues_cusips = map.merge(reissues_info[pd.notnull(reissues_info['reissue_ISIN'])].drop('reissue_ISIN', axis = 1).reset_index(drop = True),
                                                            how = 'left',
                                                            left_on = ['reissue_CUSIP'],
                                                            right_on = ['reissue_CUSIP'])
reissues = reissues_isins
for col in reissues.columns:
    reissues[col].fillna(reissues_cusips[col], inplace=True)
reissues.shape

## join the issues/reissues together
## there's a problem here: there are nan records in some cases.
# e.g. there's an expISIN and expCUSIP and an expISIN but not a expCUSIP
issues.shape
issues[pd.isnull(issues['exp_CUSIP'])]
issues[['exp_ISIN', 'exp_CUSIP', 'reissue_ISIN', 'reissue_CUSIP']].drop_duplicates().sort_values('exp_CUSIP').iloc[:50]

# since it appears like there arne't any null CUSIPs, just merge issue and reissue on that:
# UGH! THis is a mess. write it out properly.
out2 = issues_cusips.merge(reissues[pd.notnull(reissues['reissue_CUSIP'])].drop(['exp_ISIN', 'reissue_ISIN', 'months_to_reissue'], axis = 1).reset_index(drop = True),
                        how = 'inner',
                        left_on = ['exp_CUSIP', 'reissue_CUSIP'],
                        right_on = ['exp_CUSIP', 'reissue_CUSIP'])


issues[issues['exp_CUSIP'] == 'CP5084266']


.merge(reissues[pd.notnull(reissues['reissue_CUSIP'])].drop(['exp_ISIN', 'reissue_ISIN', 'months_to_reissue'], axis = 1).reset_index(drop = True),

### ed here.

out_cusip = issues.merge(reissues[pd.notnull(reissues['reissue_CUSIP'])].drop(['exp_ISIN', 'reissue_ISIN', 'months_to_reissue'], axis = 1).reset_index(drop = True),
                        how = 'inner',
                        left_on = ['exp_CUSIP', 'reissue_CUSIP'],
                        right_on = ['exp_CUSIP', 'reissue_CUSIP'])
out_isin = issues.merge(reissues[pd.notnull(reissues['reissue_ISIN'])].drop(['exp_CUSIP', 'reissue_CUSIP', 'months_to_reissue'], axis = 1).reset_index(drop = True),
                        how = 'inner',
                        left_on = ['exp_ISIN', 'reissue_ISIN'],
                        right_on = ['exp_ISIN', 'reissue_ISIN'])
out = pd.concat([out_isin, out_cusip], axis = 0)
out.shape

## now I should have a dataset that works: out
## JOIN IN the price data!
## But there's something weird going on here with the merge.
# is inner the right move?
# let's follow one pair.
in_scope = pairlist[(pairlist['exp_CUSIP'].isin(cusip_df[pd.notnull(cusip_df['id_cusip'])]['id_cusip'].unique())) & (pairlist['reissue_CUSIP'].isin(cusip_df[pd.notnull(cusip_df['id_cusip'])]['id_cusip'].unique()))]
cusip_df[pd.isnull(cusip_df['maturity_price'])]
sample_index = 0
exp_CUSIP = in_scope['exp_CUSIP'].iloc[sample_index]
reissue_CUSIP = in_scope['reissue_CUSIP'].iloc[sample_index]

pd.DataFrame([x.split('/')[-1] for x in cusip_list]).to_csv(dir + 'cusip_list.csv')

# all the ones in cusip_df are null
cusip_df[cusip_df['id_cusip'] == exp_CUSIP]['maturity_price'].unique()
# so go back and check te things that make cusip_df.
cusip_df[cusip_df['id_cusip'] == exp_CUSIP]['maturity_price'].unique()
cusip_df['id_cusip'][:5]
in_scope
issues_cusips['maturity_price'].unique()
reissues_cusips['reissue_maturity_price'].unique()
out_cusip['maturity_price'].unique()
reissues_info['maturity_price'].unique()
cusip_df['maturity_price'].unique()
out['maturity_price'].unique()

isin_dfs['px_last'].unique()
cusip_df['px_last'].unique()
out['px_last'].unique()

out[(pd.notnull(out['maturity_price'])) & (pd.notnull(out['reissue_issue_px']))]



reissue_isins_info.head()
pairlist[pairlist['exp_ISIN'].isin(included_isins)].shape
reissue_isins_info[reissue_isins_info['exp_ISIN'].isin(included_isins)]
## this says that none of the isins that have holdings data have been reissued.
## this is a problem for me.

# join in the info for the isins that follow the expiring ones.
a = a.merge(reissue_isins_info,
            left_on = ['isin'],
            right_on = ['exp_ISIN'])
a.head()

# industries = ['Sovereigns', 'Government Development Banks', 'Government Regional', 'Government Agencies']
# a = a[a['INDUSTRY'] == industries[0]].reset_index(drop = True)

sns.scatterplot(data = a, x = 'diff', y = 'hhi', hue = 'ticker')

# a[pd.isnull(a['CUR_YLD'])]#.head()
d = a[(a['CPN'] > 3.5) & (a['CPN'] < 4.5) & (a['HHI'] > 15) & (a['CURR'] == 'MXN')].reset_index(drop = True)
d
sns.scatterplot(data = a[a['TICKER'] == 'MBOND'], x = 'DIFF', y = 'HHI', hue = 'TICKER')

# d['DIFF'] = d['CPN'].subtract(d['YTM'])
d.head()
sns.scatterplot(data = d[d['TICKER'] == 'MBOND'], x = 'CUR_YLD', y = 'HHI', hue = 'TICKER')
sns.scatterplot(data = d, x = 'CUR_YLD', y = 'HHI', hue = 'TICKER')
sns.scatterplot(data = d, x = 'CPN', y = 'HHI', hue = 'TICKER')

# the ones that have varying HHIs all have YTM around 4. Their CPN varies
# and they're mostly of short maturity, <= 5y. They're all MBONDs.

# Protest Count by Country and Social Insurance Level
# sns.set_style('whitegrid')
g = sns.jointplot(data = a,
                    x = 'CPN', y = 'HHI',
                    # hue = 'TICKER',
                    hue = 'BB_COMPOSITE',
                    # hue = 'CURR_TYPE',
                    xlim = (-1,  15),
                    ylim = (-1, 100)
                    )
plt.subplots_adjust(top=0.9)
g.set_axis_labels('Social Insurance to GDP (%)', 'Monthly Weighted Protest Participants')
# plt.legend(bbox_to_anchor=(1.01, 1),borderaxespad=0)
# g.fig.suptitle(f'Protest Count by Social Insurance Level')
plt.tight_layout()
countryprotestname = dir + f'/LaTeX/protestsbycountry.png'
plt.savefig(countryprotestname)


###



def get_data_for_security_old(id, type):
    '''
    Find relevant attributes of a given security.
    '''
    # id = isin.split('/')[-1]
    # fn = self.dir + f'data_{id}.csv'

    try:
        # find the data for the list of fields we care about.
        # id = isin_list[0]
        type = 'isin'
        print(id)
        print(type)
        temp = blp.bdp(tickers = id, flds = field_list)
        print(temp)

        if ('issue_px' not in temp.columns) or (pd.isnull(temp['issue_px'].iloc[0])):
            print('hi')

            if type == 'cusip':
                cusip = id.split('/')[-1]
                issue_date_str = j[j['Bloomberg ID'] == cusip]['Issue Date'].iloc[0]
                mat_date_str = j[j['Bloomberg ID'] == cusip]['Maturity Date'].iloc[0]

            elif type == 'isin':
                isin = id.split('/')[-1]
                issue_date = j[j['ISIN'] == isin]['Issue Date'].iloc[0]
                mat_date = j[j['ISIN'] == isin]['Maturity Date'].iloc[0]

            issue_date_str = datetime.datetime.strftime(issue_date, '%Y/%m/%d')
            one_mo_after = pd.to_datetime(issue_date_str) + pd.Timedelta(7, 'D')
            one_mo_after_str = datetime.datetime.strftime(one_mo_after, '%Y/%m/%d')

            issue_price = blp.bdh(tickers = id, flds = 'PX_OPEN', start_date = issue_date_str, end_date = issue_date_str, Per = 'D', Fill='P', Days='A')
            if issue_price.shape[1] == 0:
                issue_price = blp.bdh(tickers = id, flds = 'PX_LAST', start_date = issue_date_str, end_date = issue_date_str, Per = 'D', Fill='P', Days='A')
            issue_price = issue_price.iloc[0,0]
            print(f'issue price is {issue_price}')
            temp['issue_price'] = issue_price

            # mat_date_str = datetime.datetime.strftime(mat_date, '%Y/%m/%d')
            # one_mo_before = pd.to_datetime(mat_date_str) - pd.Timedelta(7, 'D')
            # one_mo_before_str = datetime.datetime.strftime(one_mo_before, '%Y/%m/%d')
            #
            # mty_price = blp.bdh(tickers = id, flds = 'PX_OPEN', start_date = mat_date_str, end_date = mat_date_str, Per = 'D', Fill='P', Days='A')
            # if mty_price.shape[1] == 0:
            #     mty_price = blp.bdh(tickers = id, flds = 'PX_LAST', start_date = mat_date_str, end_date = mat_date_str, Per = 'D', Fill='P', Days='A')
            # mty_price = mty_price.iloc[0,0]
            # print(f'mty price is {mty_price}')
            # temp['maturity_price'] = mty_price

            amt_issued = blp.bdp(tickers = id, flds = 'AMT_ISSUED')
            amt_issued = amt_issued.iloc[0,0]
            print(f'amt issued is {amt_issued}')
            temp['amt_issued'] = amt_issued

            # alternate arrangement: avg of price over the last month before maturity.
            # maturity_price = blp.bdh(tickers = id, flds = 'PX_OPEN', start_date = issue_date_str, end_date = one_mo_after_str, Per = 'W', Fill='P', Days='A').iloc[0,0]

            temp['country'] = ctry.upper()
            temp['Mty (Yrs)'] = len(pd.date_range(end = temp['maturity'][0], start = temp['issue_dt'][0], freq = 'M')) / 12

            print(temp)
        # save the file.
        # fn = self.dir + f'data_{id}.csv'
        # temp.to_csv(fn)
        return temp
    except:
        print('Retrieval failed. Moving on.')
        pass
