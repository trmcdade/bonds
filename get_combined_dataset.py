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
from functools import reduce
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn import preprocessing


class CombineBondsData:


    def __init__(self, ctry, overwrite_data, repull_data, pull_date,
                    write_out_bond_info = None):
        '''
        Author: Timothy R. McDade
        Date: 30 Aug 2021

        This code takes the price data and finds the ISINs for the securities.
        It then adds some characteristics of the bonds taken from the Bloomberg
        Issuance data set.

        This code consolidates all the other code I had written to pull
        California data on:

        1. Security attributes. Pulled from Bloomberg in this code.
        2. Security historical pricing. Sourced from "cali1_prices.py".
        3. Security historical ownership concentration. Sourced from holdings
           data pulled in "pull_cali_holdings.py".

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
        self.repull_data = repull_data
        self.overwrite_data = overwrite_data
        self.write_out_bond_info = write_out_bond_info
        self.pull_date = pull_date
        self.dir = f'C:/Users/trmcd/OneDrive/Duke/Papers/Bonds/Data/FactSet/'

        # some dictionaries for mapping later on.
        self.month_to_quarter_dict = {'01':'1', '02':'1', '03':'1',
                                        '04':'2', '05':'2', '06':'2',
                                        '07':'3', '08':'3', '09':'3',
                                        '10':'4', '11':'4', '12':'4'}


    def get_bond_info(self):
        '''
        This function imports the CA muni bond info from 2002-present,
        downloaded from the from Bloomberg Terminal
        (SRCH --> My Searches --> "CA_MUNI").
        '''

        # info = pd.read_excel(bonds.dir + '../Bloomberg/California/ca_muni_since_2002_info.xlsx', sheet_name = 'Fixed Values')
        # info = info[info['CUSIP'] == '13063A5G5']
        # info['State'] = 'CA'
        # info['Mty (Yrs)'] = len(pd.date_range(end = info['Maturity'][0], start = info['Issue Date'][0], freq = 'M')) / 12
        # info['dtm'] = [x.days for x in (pd.to_datetime(info['Maturity']) - dt.datetime.today())]
        # info['still_outstanding'] = [1 if x > 0 else 0 for x in info['dtm']]

        self.info = pd.read_excel(self.dir + '../Bloomberg/California/ca_muni_since_2002_info.xlsx', sheet_name = 'Fixed Values')

        if self.write_out_bond_info in ['txt', 'csv']:
            cusip_list = [i for i in self.info['CUSIP']]
            if self.write_out_bond_info == 'csv':
                pd.Series(cusip_list).to_csv('C:/Users/trmcd/OneDrive/Duke/Papers/Bonds/Data/Bloomberg/California/cali_cusip_list.csv')
            if self.write_out_bond_info == 'txt':
                with open('C:/Users/trmcd/OneDrive/Duke/Papers/Bonds/Data/Bloomberg/California/cali_cusip_list.txt', 'w') as f:
                    for item in cusip_list:
                        f.write("%s\n" % item)

        self.info = self.info[self.info['ISIN'].isin(self.holdings['isin'])]
        # self.info = self.info[self.info['CUSIP'] == '13063A5G5']

        self.info['State'] = f'{self.ctry.upper()}'
        delta = pd.to_datetime(self.info['Maturity']) - pd.to_datetime(self.info['Issue Date'])
        self.info['Mty (Yrs)'] = [divmod(x.total_seconds(), 31536000)[0] for x in delta]
        # self.info['Mty (Yrs)'] = len(pd.date_range(end = self.info['Maturity'][0], start = self.info['Issue Date'][0], freq = 'M')) / 12
        self.info['still_outstanding'] = [1 if x > dt.datetime.today() else 0 for x in pd.to_datetime(self.info['Maturity'])]
        self.isin_list = [f'/isin/{i}' for i in pd.Series(self.info['ISIN'].unique())]


    def get_pricing_info(self):
        '''
        Import the pricing data, as exported from MSRB via WRDS.
        '''
        self.prices = pd.read_csv(self.dir + '../Bloomberg/California/all_bonds_px_vol_2021-09-30.csv', index_col = 0)
        self.prices = self.prices[self.prices['CUSIP'].isin(self.info['CUSIP'])].reset_index(drop = True)
        # self.prices = self.prices[self.prices['CUSIP'] == '13063A5G5'].copy().reset_index(drop = True)
        # self.prices = self.prices.copy().reset_index(drop = True)
        self.prices['date'] = pd.to_datetime(self.prices['DATE'], format = '%Y/%m/%d')
        self.prices['year'] = [str(x).split("-")[0] for x in self.prices['date']]
        self.prices['month'] = [str(x).split("-")[1] for x in self.prices['date']]

        self.prices['Q'] = [self.month_to_quarter_dict[mm] for mm in self.prices['month']]
        self.prices['yyQq'] = [self.prices['year'].iloc[x] + '-Q' + self.prices['Q'].iloc[x] for x in range(self.prices.shape[0])]


    def get_quarter_diff(self, start, end):
        '''
        Find the time between two dates measured in quarters.
        '''
        t = pd.to_datetime(end).to_period(freq='Q') - pd.to_datetime(start).to_period(freq='Q')
        return(t.n)


    def reformat_date(dd):
        '''
        Change date format from '%Y-%m-%d' to '%Y/%m/%d'.
        Might not need this.
        '''
        return dt.datetime.strptime(dd, '%Y-%m-%d').strftime('%Y/%m/%d')


    def clean_holdings_data(self):
        '''
        Imports and cleans the holdings data from FactSet.
        # TODO: update holdings data and import the right file here.
        '''
        filenames = [self.dir + f'SQL/California/{x}' for x in os.listdir(self.dir + 'SQL/California/') if '_holdings.csv' in x and 'combined' not in x]
        self.holdings = pd.concat([pd.read_csv(x, index_col = 0) for x in filenames])

        # self.holdings = pd.read_excel(self.dir + f'13063A5G5_holdings.xlsx')
        # self.holdings = pd.read_csv(self.dir + 'SQL/California/combined_holdings.xlsx')
        # self.holdings = self.holdings[self.holdings['isin'] == 'US13063A5G50'].copy().reset_index(drop = True)

        self.holdings = self.holdings[pd.notnull(self.holdings['report_date'])].copy().reset_index(drop = True)
        self.holdings['report_date'] = [dt.datetime.strftime(pd.to_datetime(x).replace(day=1), '%Y/%m/%d') for x in tqdm(self.holdings['report_date'])]
        self.holdings['Q'] = [self.month_to_quarter_dict[x.split('/')[1]] for x in self.holdings['report_date']]
        self.holdings['year'] = [x.split('/')[0] for x in self.holdings['report_date']]
        self.holdings['yyQq'] = [self.holdings['year'].iloc[x] + '-Q' + self.holdings['Q'].iloc[x] for x in range(self.holdings.shape[0])]

        self.holdings = self.holdings[['isin', 'Managing Inst. Id', 'Managing Inst.', 'report_date', 'Managing Inst. Reported Holding', 'year', 'Q', 'yyQq']].copy()
        self.holdings = self.holdings.drop(['report_date'], axis = 1)
        self.holdings = self.holdings.groupby([x for x in self.holdings.columns if x != 'Managing Inst. Reported Holding']).sum().sort_values(['yyQq', 'Managing Inst. Reported Holding'], ascending = False).reset_index()


    def start_bbg_session(self):
        '''
        Start the Bloomberg API session.
        '''
        options = blpapi.SessionOptions()
        options.setServerHost('localhost')
        options.setServerPort(8194)
        session = blpapi.Session(options)
        session.start()
        print('Bloomberg session started.')


    def get_data_for_isin(self, isin):
        '''
        THIS HITS THE BLOOMBERG API.
        ONLY RUN IT IF YOU ACTUALLY NEED TO PULL THE DATA!!
        '''
        try:
            id = isin.split('/')[-1]
            fn = self.dir + f'../Bloomberg/California/bond_info_with_aol_{id}.csv'
            temp = self.info[self.info['ISIN'] == isin.split('/')[2]]

            # get the dates for which you want the amt outstanding.
            # earlier I had done all monthly dates. now, change to quarterly
            # to preserve hits on the API.
            # date_list = pd.date_range(pd.to_datetime(temp['Issue Date'].iloc[0]), pd.to_datetime(temp['Maturity'].iloc[0]), freq = 'MS')
            date_list = pd.date_range(pd.to_datetime(temp['Issue Date'].iloc[0]), pd.to_datetime(temp['Maturity'].iloc[0]), freq = 'QS')
            date_list = [dt.datetime.strftime(x, '%Y/%m/%d') for x in date_list if x <= pd.to_datetime("today")]
            date_list_for_query = [x.replace("/","") for x in date_list]

            # pull the amt outstanding for each of those dates.
            ao = [blp.bdp(tickers = isin, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd) for dd in date_list]
            # ao = [blp.bdp(tickers = isin, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd) for dd in date_list_for_query]
            aol = [x['amt_outstanding'][0] if x.shape[0] > 0 else np.NaN for x in ao]
            # reshape temp to be able to take that data.
            temp = pd.concat([temp] * len(aol), ignore_index = True)
            temp['date'] = date_list
            temp['year'] = [x.split("/")[0] for x in temp['date']]
            temp['month'] = [x.split("/")[1] for x in temp['date']]
            temp['amt_outstanding'] = aol
            # export the file per bond.
            temp.to_csv(fn)
            return temp
        except:
            print(f'Bloomberg pull failed for ISIN {isin}.')
            pass


    def pull_bbg_if_necessary(self):
        '''
        First, see if we already have the BBG data for the ISINs in question.
        Then see if the ISIN has no holdings data available.
        If not, pull the BBG data.
        If there's still a problem with the Amt Out data, re-pull what's missing.
        '''
        all_files = [self.dir + f'../Bloomberg/California/{x}' for x in os.listdir(self.dir + '../Bloomberg/California/') if x.startswith('bond_info_with_aol_') and x.endswith('.csv')]
        isins_already_done = [x.split('/')[-1].split('_')[-1].split('.')[0] for x in all_files]
        isins_still_to_do = [x.split('/')[2] for x in self.isin_list if x.split('/')[2] not in isins_already_done]
        if self.overwrite_data == 1: # if we want to over-write already pulled data
            isins_still_to_do = [x for x in isins_already_done if x in [x.split('/')[2] for x in self.isin_list]] + isins_still_to_do

        # only pull bloomberg data for the files that actually have holdings data.
        drop_isins_list = []
        for ii in isins_still_to_do:
            temp = self.holdings[self.holdings['isin'] == ii]
            if temp.shape[0] == 0:
                drop_isins_list.append(ii)
            elif temp['Managing Inst. Reported Holding'].all() == 0:
                drop_isins_list.append(ii)
            elif pd.isnull(temp['Managing Inst. Reported Holding'].all()):
                drop_isins_list.append(ii)

        # now remove the drop isins from the still to do list.
        isins_still_to_do = [x for x in isins_still_to_do if x not in drop_isins_list]
        files_still_to_do = [x for x in all_files if x.split('/')[-1].split('_')[-1].split('.')[0] in isins_still_to_do]
        num_to_do = len(files_still_to_do)

        if len(isins_still_to_do) > 0:
            # if there are any files that are missing, pull the data.
            print(f'Pulling Bloomberg data for {num_to_do} isins.')
            self.start_bbg_session()
            list_of_dfs = [self.get_data_for_isin(f'/isin/{ii}') for ii in tqdm(isins_still_to_do)]
            # check if Amt Out needs to be fixed for each file, fix if needed.
            if self.repull_data == 1:
                for ff in tqdm(files_still_to_do):
                    temp = pd.read_csv(ff, index_col = 0)
                    if temp['amt_outstanding'].all() == 0:
                        isin = temp['ISIN'].iloc[0]
                        print(f'Pulling Amt Out again for ISIN {isin}.')
                        id = f'/isin/{isin}'
                        date_list = pd.date_range(temp['Issue Date'][0], temp['Maturity'][0], freq = 'MS')
                        date_list = [dt.datetime.strftime(x, '%Y/%m/%d') for x in date_list if x <= pd.to_datetime("today")]
                        # try a different format for the date for some securities.
                        date_list_for_query = [x.replace("/","") for x in date_list]
                        aol = [blp.bdp(tickers = id, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd) for dd in date_list_for_query]
                        aol = [x['amt_outstanding'][0] if x.shape[0] > 0 else np.NaN for x in aol]
                        if not all(aol) == 0:
                            print('Success with ISIN {isin}.')
                            temp['amt_outstanding'] = aol
                            temp.to_csv(ff)
                        else:
                            pass
                    else:
                        pass
        else:
            print('All data already exists. Not pulling anything new or fixing anything.')
        # then, since we re-wrote some of the data, re-define what 'out' is.
        info = pd.concat([pd.read_csv(x, index_col = 0) for x in all_files])
        info = info[info['ISIN'].isin([x.split('/')[2] for x in self.isin_list])]
        info['year'] = info['year'].astype('str') # make the year a string
        info['month'] = info['month'].astype('str').str.rjust(2, '0') # make the month a string and zero-pad it.

        ordered_cols = ['Bloomberg ID', 'CUSIP', 'ISIN', 'Issuer Name', 'State',
                        'Currency', 'BBG Composite', 'Dated Date', 'Issue Date',
                        'Maturity', 'Mty Size', 'Amt Out', 'Issue Price',
                        'Yield at Issue', 'Cpn', 'Coupon Type', 'Last MSRB Avg Px',
                        'mty_typ',
                        'still_outstanding', 'Mty (Yrs)',
                        'date', 'year', 'month', 'amt_outstanding']
        info = info[[x for x in ordered_cols if x in info.columns]].reset_index(drop = True)

        self.info = info[ordered_cols].reset_index(drop = True)
        self.info['Q'] = [self.month_to_quarter_dict[x.split('/')[1]] for x in self.info['date']]
        self.info['yyQq'] = [self.info['year'].iloc[x] + '-Q' + self.info['Q'].iloc[x] for x in range(self.info.shape[0])]
        self.info = self.info.drop(['year', 'month', 'Q'], axis = 1) # so we don't have duplicate columns


    def merge_pricing_and_security_data(self):
        '''
        Merge the pricing data p with the other security info g.
        '''
        self.px_and_info = self.prices.merge(self.info, on = ['CUSIP', 'yyQq'])
        self.px_and_info = self.px_and_info.rename(columns = {'date_x':'date'}).drop(['date_y', 'DATE'], axis = 1)
        self.px_and_info['date'] = pd.to_datetime(self.px_and_info['date'])
        self.px_and_info['date_q'] = pd.PeriodIndex(self.px_and_info['date'], freq='Q').to_timestamp()
        self.px_and_info['qtm'] = [self.get_quarter_diff(self.px_and_info['date'].iloc[ii], self.px_and_info['Maturity'].iloc[ii]) for ii in range(self.px_and_info.shape[0])]
        self.px_and_info['dtm'] = [x.days for x in (pd.to_datetime(self.px_and_info['Maturity']) - pd.to_datetime(self.px_and_info['date']))]
        self.px_and_info.to_csv(self.dir + 'SQL/California/combined_px_and_info.csv')


    def calc_hhi(self):
        '''
        Now, calculate HHI for all dates.
        Remember to filter by report_date in the holdings data before or
        equal to the Amt_out date.

        TODO: there's something wrong here. Maybe there are duplicate records
        by quarter, and the reported holdings appear to be larger than the amt
        outstanding.
        '''
        hhi_fn = f'{self.ctry}_bond_hhi_data.csv'
        # Change the > to < if you need to manually override.
        if len([x for x in os.listdir(self.dir) if x == hhi_fn]) > 0:
            print('HHI data already exists. Moving on.')
            hhi_df = pd.read_csv(self.dir + hhi_fn, index_col = 0)
        else:
            print('Calculating the HHI for each security over time.')
            list_of_dicts = []
            for ii in tqdm(self.px_and_info['ISIN'].unique()):
                # use the holdings data for the isin.
                temp_holdings = self.holdings[self.holdings['isin'] == ii]
                temp_px_and_info = self.px_and_info[self.px_and_info['ISIN'] == ii][['ISIN', 'amt_outstanding', 'year', 'Q', 'yyQq']].drop_duplicates().reset_index(drop = True)
                if not temp_px_and_info['amt_outstanding'].all() == 0:
                    combined = temp_holdings.merge(temp_px_and_info[['ISIN', 'amt_outstanding', 'year', 'Q', 'yyQq']],
                                            left_on = ['isin', 'year', 'Q', 'yyQq'],
                                            right_on = ['ISIN', 'year', 'Q', 'yyQq']
                                            )
                    for dd in combined['yyQq'].unique():
                        di_temp = combined[combined['yyQq'] == dd].copy()
                        di_temp['pct_os'] = di_temp['Managing Inst. Reported Holding'].divide(di_temp['amt_outstanding'])
                        pct_os_accounted_for = di_temp['pct_os'].sum()
                        di_temp['pct_os_sq'] = di_temp['pct_os'] ** 2
                        hhi = di_temp['pct_os_sq'].sum()
                        hhi_temp_out = dict()
                        hhi_temp_out['isin'] = ii
                        hhi_temp_out['yyQq'] = dd
                        hhi_temp_out['hhi'] = hhi
                        hhi_temp_out['pct_os_known'] = pct_os_accounted_for
                        list_of_dicts.append(hhi_temp_out)
            hhi_df = pd.DataFrame(list_of_dicts)
            hhi_df['date'] = pd.PeriodIndex(hhi_df['yyQq'], freq='Q').to_timestamp()
            hhi_df.to_csv(self.dir + hhi_fn)
        return hhi_df


    def get_quarterly_smoothed_volatility(self, df):
        col_list = [x for x in df.columns if 'vol_' in x or 'close_' in x or 'log_ret' in x]
        outlist = []
        for col in col_list:
            temp = df[['date_q', col]].groupby(['date_q']).mean().reset_index()
            outlist.append(temp)
        # merge these together and output them
        df_merged = reduce(lambda  left,right: pd.merge(left, right, on = ['date_q'], how = 'outer'), outlist)
        # col_name_dict = {k: f'{k}_q' if 'vol_' in k else k for k in df_merged.columns}
        col_name_dict = {k: f'{k}_q' if k in col_list else k for k in df_merged.columns}
        df_merged = df_merged.rename(columns = col_name_dict)
        return df_merged


    def main(self):
        '''
        TODO: I have some empty yyqq and some empty ISINs.
        '''
        print('Beginning the pull.')

        print('Cleaning holdings data.')
        self.clean_holdings_data()
        print('Holdings data cleaned.')

        print('Importing bond information.')
        self.get_bond_info()
        print('Bond information imported.')

        print('Importing pricing information.')
        self.get_pricing_info()
        print('Pricing information imported.')

        print('Retrieving financial data.')
        self.pull_bbg_if_necessary()
        print('Financial data retrieved.')

        print('Combining financial data.')
        self.merge_pricing_and_security_data()
        print('Financial data combined.')

        print('Calculating ownership concentration.')
        self.hhi_df = self.calc_hhi()
        print('Ownership concentration calculated.')

        print('Creating final data set.')
        self.px_and_info = self.px_and_info[self.px_and_info['date'] >= self.hhi_df['date'].min()]
        self.hhi_df = self.hhi_df.drop('date', axis = 1)
        self.out_df = self.px_and_info.merge(self.hhi_df,
                                                how = 'left',
                                                left_on = ['ISIN', 'yyQq'],
                                                right_on = ['isin','yyQq']
                                                )
        self.out_df = self.out_df[~pd.isnull(self.out_df['ISIN'])]
        # create smoothed quarterly volatility numbers
        smoothed_vol = self.get_quarterly_smoothed_volatility(self.out_df)
        self.out_df = self.out_df.merge(smoothed_vol, on = 'date_q')
        self.out_df.to_csv(self.dir + f'{self.ctry}_reg_data_{self.pull_date}.csv')
        print('All done.')



if __name__ == "__main__":

    bonds = CombineBondsData(ctry = 'CA',
                                overwrite_data = 0,
                                repull_data = 0,
                                write_out_bond_info = None,
                                pull_date = '2021-09-30')
    bonds.main()


# TODO: see if the ones w amt_outstanding == 0 have any data in bbg
# or have an amt issued. If so, replace the amt_outstanding with amt_issued.
# I can use the Mty Size field, which represents the initial principal value,
# but it isn't always a good substitute because some bonds are callable.

#
