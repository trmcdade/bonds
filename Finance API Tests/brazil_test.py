import pandas as pd
import numpy as np
import datetime as dt
from tqdm import tqdm
from joblib import Parallel, delayed
import os

'''
This code takes the price data and finds the ISINs for the securities.
It then adds some characteristics of the bonds taken from the Bloomberg
Issuance data set.
'''

dir = 'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet/brazil/'
p = pd.read_csv(dir + 'bond_prices_expanded.csv', index_col = 0).reset_index(drop = True)

bond_data_fn = 'C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/Output_Data/Issues/bond_issuances_py.csv'
j = pd.read_csv(bond_data_fn, index_col = 0)
j = j[(j['Country (Full Name)'] == 'BRAZIL') & (j['Maturity (Months)'] >= 0)].copy()
j = j[j['Issue Date'] >= '2002-01-01']
# isinout = pd.DataFrame(j['ISIN'].unique(), columns = ['Identifiers'])
# isinout['Trading platform (id)'] = 183 # SELIC
# isinout.iloc[:5].to_excel(dir + 'isin_list.xlsx')

# g = j[(pd.notnull(j['ISIN'])) & (j['Curr'] == 'BRL')][['ISIN', 'Bloomberg ID', 'Issuer Name', 'Security Name', 'Issue Date', 'Maturity Date']].copy().reset_index(drop = True)
g = j[(pd.notnull(j['ISIN']))][['ISIN', 'Bloomberg ID', 'Issuer Name', 'Security Name', 'Issue Date', 'Maturity Date']].copy().reset_index(drop = True)
g['stem'] = g['ISIN'].str[:-3]
map = {'BRSTNCLF1' : 'LFT',
        'BRSTNCLTN' : 'LTN',
        'BRSTNCNTB' : 'NTN-B',
        'BRSTNCNTF' : 'NTN-F',
        'BRSTNCNTC' : 'NTN-C',
        'BRSTNCNTD' : 'NTN-D'
        }
g = g[g['stem'].isin(map.keys())].copy().reset_index(drop = True)
g['type'] = [map[x] for x in g['stem']]
g = g[g['type'] != 'NTN-D'].copy().reset_index(drop = True)
g['mat_year'] = [x.split('-')[0] for x in g['Maturity Date']]
g['mat_month'] = [x.split('-')[1] for x in g['Maturity Date']]
g['mat_day'] = [x.split('-')[2] for x in g['Maturity Date']]
g[['mat_year', 'mat_month', 'mat_day']] = g[['mat_year', 'mat_month', 'mat_day']].astype('int')

def get_px_yd(asset_code):
    # asset_code = 'LFT 210104'
    # asset_code = sorted(p['asset.code'].unique())[2]
    out = p[(p['asset.code'] == asset_code)].sort_values('ref.date')

    if out.shape[0] > 0:
        if out['matur.date'].unique()[0] >= dt.datetime.strftime(dt.datetime.today(), '%Y-%m-%d'): # hasn't matured yet
            still_outstanding = 1
            days_to_mty = (pd.to_datetime(out['matur.date'].unique()[0]) - dt.datetime.today()).days
        else:
            still_outstanding = 0
            days_to_mty = (pd.to_datetime(out['matur.date'].unique()[0]) - dt.datetime.today()).days

        (issue_px, issue_yd) = out[['price.bid', 'yield.bid']].iloc[0]
        (mat_px, mat_yd) = out[['price.bid', 'yield.bid']].iloc[-1]

    else:
        (issue_px, issue_yd, mat_px, mat_yd) = (np.NaN, np.NaN, np.NaN, np.NaN)

    return (asset_code, issue_px, issue_yd, mat_px, mat_yd, still_outstanding, days_to_mty)


## figure this out: de-dupe because they have lots of taps and reissues.
ll = []
for isin in g['ISIN'].unique():
    temp = g[g['ISIN'] == isin].sort_values('Issue Date')#.iloc[0]
    temp = temp.groupby([x for x in temp.columns if x != 'Issue Date']).min().reset_index()
    if temp.shape[0] > 0:
        ll.append(temp)
g = pd.concat(ll).reset_index(drop = True)

# merge the pricing data p with the other security info g.
# p[['asset.code', 'yyqq']].drop_duplicates().sort_values(['yyqq', 'asset.code'])
datediff = pd.to_datetime(g['Maturity Date']) - pd.to_datetime(g['Issue Date'])
g['mty_length_days'] = [x.days for x in datediff]
df = p.merge(g, how = 'left',
                left_on = ['type', 'mat_year', 'mat_month', 'mat_day'],
                right_on = ['type', 'mat_year', 'mat_month', 'mat_day'])
df = df[[x for x in g.columns if x != 'Issuer Name'] + [x for x in p.columns]]

# most of the asset.codes have ISINs.
# df[pd.notnull(df['ISIN'])]['asset.code'].unique()
# df[pd.isnull(df['ISIN'])]['asset.code'].unique()

pricesandyields = Parallel(n_jobs = int(os.cpu_count()))(delayed(get_px_yd)(ac) for ac in tqdm(df['asset.code'].unique()))
maturity_info = pd.DataFrame(pricesandyields, columns = ['asset.code', 'issue_px', 'issue_yd', 'mat_px', 'mat_yd', 'still_outstanding', 'days_to_mty'])

# df contains all the price info for the available bonds and their price at maturity.
# I can then drop all the other records for the columns not of interest so I just
# have the ids and the prices at maturity.

# there are a ton fewer bonds in p than in g because it's only Brazil Treasury
# bonds observed after 2002. It's only the ones denominated in BRL.

# but for some reason it leaves out 2011 pricing data?
# df[pd.notnull(df['ISIN'])][['ISIN', 'yyqq', 'p.mean', 'y.mean', 'p.sd', 'y.sd']].drop_duplicates()

df['ref.date'] = df.loc[:,'ref.date'].astype('datetime64[ns]')
df['matur.date'] = df.loc[:,'matur.date'].astype('datetime64[ns]')
df['Maturity Date'] = df.loc[:,'Maturity Date'].astype('datetime64[ns]')
df['Issue Date'] = df.loc[:,'Issue Date'].astype('datetime64[ns]')

df_new = df.merge(maturity_info, how = 'left', on = 'asset.code')

# what's the deal here?
df[['ISIN', 'yyqq', 'p.mean', 'y.mean', 'p.sd', 'y.sd']].drop_duplicates()
# i think the problem is that some of the secuities in p don't have
# matches in g. That means they were issued 2018 or later?
df_new[['Maturity Date', 'Issue Date', 'mty_length_days']]

df_new.to_csv(dir + 'price_data_for_join_to_pairlist.csv')
'''This output goes to get_bb_data_for_brazil_2.py'''
