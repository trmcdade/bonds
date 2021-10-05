import pandas as pd
import numpy as np
import datetime as dt
import sys
import os
from os import listdir
import blpapi
from xbbg import blp
from tqdm import tqdm
from joblib import Parallel, delayed

'''
This code takes the price data and finds the ISINs for the securities.
It then adds some characteristics of the bonds taken from the Bloomberg
Issuance data set.
'''

class CombineBondsData:


    def __init__(self, ctry):
        '''
        Author: Timothy R. McDade
        Date: 29 June 2021
        This code consolidates all the other code I had written to pull
        Brazil's data on:
        1. Security attributes
        2. Security historical pricing
        3. Security historical ownership concentration

        The general work flow is:
        1. Clean the ownership data.
        2. Find the ISINs for the pricing data, which only comes with security names.
        3. Combine the daily security pricing data with its unchanging attributes
           such as issue_px, mat_px, etc.
        4. Merge the pricing data with the other miscellaneous security data
           sourced from Bloomberg (e.g. coupons, blah blah.)
        5. Pull more data from Bloomberg to supplement this, if necessary.
        6. Calculate the Hirschman-Hirfindahl Index of ownership concentration
           for each security.
        7. Merge together the final data set and output it.
        '''

        self.ctry = ctry
        self.dir = f'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet/{ctry}/'
        self.p = pd.read_csv(dir + 'bond_prices_expanded.csv', index_col = 0).reset_index(drop = True)

        # the issue-level security data is housed in another place:
        bond_data_fn = 'C:/Users/trmcd/Dropbox/Debt Issues and Elections/Tim_Code/Output_Data/Issues/bond_issuances_py.csv'
        j = pd.read_csv(bond_data_fn, index_col = 0)
        j = j[(j['Country (Full Name)'] == self.ctry.upper()) & (j['Maturity (Months)'] >= 0)].copy()
        self.j = j[j['Issue Date'] >= '2002-01-01']

        # some dictionaries for mapping later on.
        self.month_to_quarter_dict = {'1':'1', '2':'1', '3':'1',
                                        '4':'2', '5':'2', '6':'2',
                                        '7':'3', '8':'3', '9':'3',
                                        '10':'4', '11':'4', '12':'4'}
        self.month_to_quarter_dict2 = {'01':'1', '02':'1', '03':'1',
                                        '04':'2', '05':'2', '06':'2',
                                        '07':'3', '08':'3', '09':'3',
                                        '10':'4', '11':'4', '12':'4'}


    def get_quarter_diff(start, end):
        '''
        Find the time between two dates measured in quarters.
        '''
        t = pd.to_datetime(end).to_period(freq='Q') - pd.to_datetime(start).to_period(freq='Q')
        return(t.n)


    def reformat_date(dd):
        '''
        Change date format from '%Y-%m-%d' to '%Y/%m/%d'.
        '''
        return datetime.datetime.strptime(dd, '%Y-%m-%d').strftime('%Y/%m/%d')


    def clean_holdings_data(self):
        '''
        Imports and cleans the holdings data from FactSet.
        '''
        self.z = pd.read_csv(dir + f'{ctry}_fixed_income.csv')
        self.z = self.z[pd.notnull(self.z['report_date'])].copy().reset_index(drop = True)
        self.z['report_date'] = [datetime.datetime.strftime(pd.to_datetime(x).replace(day=1), '%Y/%m/%d') for x in tqdm(self.z['report_date'])]
        self.z['Q'] = [month_to_quarter_dict2[x.split('/')[1]] for x in self.z['report_date']]
        self.z['year'] = [x.split('/')[0] for x in self.z['report_date']]
        self.z['yyQq'] = [self.z['year'].iloc[x] + '-Q' + self.z['Q'].iloc[x] for x in range(self.z.shape[0])]
        self.z = self.z[['isin', 'factset_entity_id', 'entity_proper_name', 'report_date', 'reported_holding', 'year', 'Q', 'yyQq']].copy()
        self.z = self.z.drop(['report_date'], axis = 1)


    def find_stems_and_mat_dates_for_matching(self):
        '''
        The pricing data, obtained from the Brazilian securities exchange B3
        via the GetTDData package in R, is not identified by ISIN. So I have
        to map the 'asset.code' (the unique identifier in the pricing data) to
        ISIN, which is what the rest of the security data (from Bloomberg) and
        the ownership data (from FactSet) use.
        '''
        self.g = self.j[(pd.notnull(self.j['ISIN']))][['ISIN', 'Bloomberg ID', 'Issuer Name', 'Security Name', 'Issue Date', 'Maturity Date']].copy().reset_index(drop = True)
        self.g['stem'] = self.g['ISIN'].str[:-3]
        map = {'BRSTNCLF1' : 'LFT',
                'BRSTNCLTN' : 'LTN',
                'BRSTNCNTB' : 'NTN-B',
                'BRSTNCNTF' : 'NTN-F',
                'BRSTNCNTC' : 'NTN-C',
                'BRSTNCNTD' : 'NTN-D'
                }
        self.g = self.g[self.g['stem'].isin(map.keys())].copy().reset_index(drop = True)
        self.g['type'] = [map[x] for x in self.g['stem']]
        self.g = self.g[self.g['type'] != 'NTN-D'].copy().reset_index(drop = True)
        self.g['mat_year'] = [x.split('-')[0] for x in self.g['Maturity Date']]
        self.g['mat_month'] = [x.split('-')[1] for x in self.g['Maturity Date']]
        self.g['mat_day'] = [x.split('-')[2] for x in self.g['Maturity Date']]
        self.g[['mat_year', 'mat_month', 'mat_day']] = self.g[['mat_year', 'mat_month', 'mat_day']].astype('int')


    def get_px_yd(self, asset_code):
        '''
        Get more permanent attributes of the security such as issue price,
        price at maturity, etc.
        TODO: delete cols still outstanding and days_to_mty
        '''
        # asset_code = 'LFT 210104'
        # asset_code = sorted(p['asset.code'].unique())[2]
        out = self.p[(self.p['asset.code'] == asset_code)].sort_values('ref.date')
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


    def combine_pricing_data(self):
        '''
        Find min date, which is Issue Date?
        '''
        ## figure this out: de-dupe because they have lots of taps and reissues.
        self.find_stems_and_mat_dates_for_matching() # results put in self.g
        ll = []
        for isin in self.g['ISIN'].unique():
            temp = self.g[self.g['ISIN'] == isin].sort_values('Issue Date')#.iloc[0]
            temp = temp.groupby([x for x in temp.columns if x != 'Issue Date']).min().reset_index()
            if temp.shape[0] > 0:
                ll.append(temp)
        self.g = pd.concat(ll).reset_index(drop = True)
        datediff = pd.to_datetime(self.g['Maturity Date']) - pd.to_datetime(self.g['Issue Date'])
        self.g['mty_length_days'] = [x.days for x in datediff]
        self.isin_list = [f'/isin/{i}' for i in pd.Series(self.g['ISIN'].unique())]


    def merge_pricing_and_security_data(self):
        '''
        Merge the pricing data p with the other security info g.

        TODO: but for some reason it leaves out 2011 pricing data?
        df[pd.notnull(df['ISIN'])][['ISIN', 'yyqq', 'p.mean', 'y.mean', 'p.sd', 'y.sd']].drop_duplicates()
        '''
        self.p[['asset.code', 'yyqq']].drop_duplicates().sort_values(['yyqq', 'asset.code'])
        # df contains all the price info for the available bonds and their price at maturity.
        df = self.p.merge(self.g, how = 'left',
                            left_on = ['type', 'mat_year', 'mat_month', 'mat_day'],
                            right_on = ['type', 'mat_year', 'mat_month', 'mat_day'])
        df = df[[x for x in self.g.columns if x != 'Issuer Name'] + [x for x in self.p.columns]]
        df['ref.date'] = df.loc[:,'ref.date'].astype('datetime64[ns]')
        df['matur.date'] = df.loc[:,'matur.date'].astype('datetime64[ns]')
        df['Maturity Date'] = df.loc[:,'Maturity Date'].astype('datetime64[ns]')
        df['Issue Date'] = df.loc[:,'Issue Date'].astype('datetime64[ns]')

        # most of the asset.codes have ISINs, but there are some that don't.
        # df[pd.notnull(df['ISIN'])]['asset.code'].unique()
        # df[pd.isnull(df['ISIN'])]['asset.code'].unique()

        pricesandyields = Parallel(n_jobs = int(os.cpu_count()))(delayed(self.get_px_yd)(ac) for ac in tqdm(df['asset.code'].unique()))
        maturity_info = pd.DataFrame(pricesandyields, columns = ['asset.code', 'issue_px', 'issue_yd', 'mat_px', 'mat_yd', 'still_outstanding', 'days_to_mty'])

        self.df_new = df.merge(maturity_info, how = 'left', on = 'asset.code')
        self.df_new = self.df_new[[x for x in self.df_new.columns if not x.endswith('.1')]]

        newdates = Parallel(n_jobs = int(os.cpu_count()))(delayed(reformat_date)(dd) for dd in tqdm(self.df_new['ref.date']))
        self.df_new['ref.date'] = newdates
        self.df_new['date'] = pd.PeriodIndex(pd.to_datetime(self.df_new['ref.date']), freq='Q').to_timestamp()
        self.df_new['qtm'] = [get_quarter_diff(self.df_new['ref.date'].iloc[ii], self.df_new['matur.date'].iloc[ii]) for ii in range(self.df_new.shape[0])]

        self.df_new.to_csv(self.dir + 'price_data_for_join_to_pairlist.csv')
        # return self.df_new


    def start_bbg_session(self):
        '''Start the Bloomberg API session.'''
        print('Bloomberg session started.')
        options = blpapi.SessionOptions()
        options.setServerHost('localhost')
        options.setServerPort(8194)
        session = blpapi.Session(options)
        session.start()


    def get_data_for_isin(self, isin):
        '''
        THIS HITS THE BLOOMBERG API.
        ONLY RUN IT IF YOU ACTUALLY NEED TO PULL THE DATA!!
        '''
        field_list = ['ID_ISIN', 'TICKER', 'Currency', 'CPN_TYP', 'BB_COMPOSITE',
                        'MATURITY', 'ISSUE_DT', 'CPN', 'YLD_YTM_BID',
                        'YLD_CUR_MID', 'PX_LAST', 'ISSUE_PX', 'REDEMP_VAL']
        self.start_bbg_session()
        try:
            temp = blp.bdp(tickers = isin, flds = field_list)
            date_list = pd.date_range(temp['issue_dt'][0], temp['maturity'][0], freq = 'MS')
            date_list = [datetime.datetime.strftime(x, '%Y/%m/%d') for x in date_list if x <= pd.to_datetime("today")]
            aol = [blp.bdp(tickers = isin, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd)['amt_outstanding'][0] for dd in date_list]

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


    def pull_bbg_if_necessary(self):
        '''
        First, see if we already have the data for the ISINs in question.
        If not, pull it.
        If there's still a problem with the Amt Out data, re-pull what's missing.
        '''
        # this is done - I can just look at the data files.
        ## Once I've pulled the security data, I can just pull the info from these files:
        fn_list = [dir + f'{x}' for x in os.listdir(dir) if x.startswith('data_') and x.endswith('.csv')]

        if len(fn_list) < len(self.isin_list):
            # if there are any files that are missing, pull the data.
            isins_still_to_do = [x for x in [x.split('.csv')[0].split('data_')[1] for x in fn_list] if x not in [x.split('/')[2] for x in isin_list]]
            num_to_do = len(isins_still_to_do)
            print(f'Pulling Bloomberg data for {num_to_do} isins.')
            list_of_dfs = [get_data_for_isin(ii) for ii in tqdm(isins_still_to_do)]
            # check if Amt Out needs to be fixed for each file, fix if needed.
            for ff in tqdm(fn_list):
                temp = pd.read_csv(dir + ff, index_col = 0)
                if temp['amt_outstanding'].all() == 0:
                    isin = temp['id_isin'].iloc[0]
                    print(f'Pulling Amt Out again for isin {isin}')
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
        else:
            print('All data already exists. Not pulling anything new or fixing anything.')
        # then, since we re-wrote some of the data, re-define what 'out' is.
        out = pd.concat([pd.read_csv(x, index_col = 0) for x in fn_list])
        out = out[[x for x in ordered_cols if x in out.columns]].reset_index(drop = True)
        ordered_cols = ['country', 'Mty (Yrs)', 'id_isin', 'ticker', 'currency',
                        'cpn_typ', 'bb_composite', 'maturity', 'issue_dt',
                        'cpn', 'yld_ytm_bid', 'yld_cur_mid', 'px_last', 'issue_px',
                        'redemp_val', 'date', 'amt_outstanding']
        self.out = out[ordered_cols].reset_index(drop = True)
        self.out['Q'] = [month_to_quarter_dict2[x.split('/')[1]] for x in self.out['date']]
        self.out['year'] = [x.split('/')[0] for x in self.out['date']]
        self.out['yyQq'] = [self.out['year'].iloc[x] + '-Q' + self.out['Q'].iloc[x] for x in range(self.out.shape[0])]
        self.out = self.out.drop(['date'], axis = 1)


    def calc_hhi(self):
        '''
        Now, calculate HHI for all dates. Remember to filter by report_date in the
        holdings data before or equal to the Amt_out date.
        '''
        hhi_fn = f'{self.ctry}_bond_hhi_data.csv'
        # CHANGE THIS BACK TO NORMAL
        if len([x for x in os.listdir(self.dir) if x == hhi_fn]) < 0:
            print('Data already exists. Moving on.')
            hhi_df = pd.read_csv(self.dir + hhi_fn, index_col = 0)
        else:
            print('Calculating the HHI for each security over time.')
            list_of_dicts = []
            for ii in tqdm(self.df_new['ISIN'].unique()):
                # use the holdings data for the isin.
                temp = self.z[self.z['isin'] == ii]
                if not out[out['id_isin'] == ii]['amt_outstanding'].all() == 0:
                    combined = temp.merge(out[['id_isin', 'amt_outstanding', 'year', 'Q', 'yyQq']],
                                            how = 'left',
                                            left_on = ['isin', 'year', 'Q', 'yyQq'],
                                            right_on = ['id_isin', 'year', 'Q', 'yyQq']
                                            )
                    for dd in combined['yyQq'].unique():
                        di_temp = combined[combined['yyQq'] == dd].copy()
                        di_temp['pct_os'] = di_temp['reported_holding'].divide(di_temp['amt_outstanding'])
                        di_temp['pct_os_sq'] = di_temp['pct_os'] ** 2
                        hhi = di_temp['pct_os_sq'].sum()
                        hhi_temp_out = dict()
                        hhi_temp_out['isin'] = ii
                        hhi_temp_out['yyQq'] = dd
                        hhi_temp_out['hhi'] = hhi
                        list_of_dicts.append(hhi_temp_out)
            hhi_df = pd.DataFrame(list_of_dicts)
            hhi_df['date'] = pd.PeriodIndex(hhi_df['yyQq'], freq='Q').to_timestamp()
            hhi_df.to_csv(dir + hhi_fn)
        return hhi_df


    def __main__(self):
        '''
        TODO: I have some empty yyqq and some empty ISINs.
        '''
        self.clean_holdings_data()
        self.combine_pricing_data()
        self.merge_pricing_and_security_data()
        self.pull_bbg_if_necessary()
        # calculate the holdings data.
        self.hhi_df = self.calc_hhi()
        # merge hhi data with the rest of the security data
        self.df_new = self.df_new[self.df_new['date'] >= self.hhi_df['date'].min()]
        self.out_df = self.df_new.merge(self.hhi_df,
                                        how = 'left',
                                        left_on = ['ISIN', 'date'],
                                        right_on = ['isin', 'date']
                                        )
        self.out_df = self.out_df[~pd.isnull(self.out_df['ISIN'])]
        self.out_df.to_csv(dir + f'{ctry}_reg_data.csv')


if __name__ == "__main__":

    bonds = CombineBondsData(ctry = 'brazil')
    bonds.main()
