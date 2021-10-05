import numpy as np
import pandas as pd
import os
from os import listdir
import pyexcel as pe
import datetime
import blpapi
from xbbg import blp
from tqdm import tqdm
from joblib import Parallel, delayed
import plotly.express as px
import sys

options = blpapi.SessionOptions()
options.setServerHost('localhost')
options.setServerPort(8194)
session = blpapi.Session(options)
session.start()


class GetBondConcentrationData:


    def __init__(self, country, pull_more_data):

        """
        Author: Tim McDade
        Date: 20 May 2021
        This class retrieves two sets of data.
        1. A list of the ISINs of government fixed-income securities in a country,
            sourced from FactSet.
        2. Data about each of those securities, sourced from Bloomberg.

        It then calculates the Hirschman-Hirfindahl Index of concentration
        for the ownership structure for these securities.

        It then outputs data for regression.
        """

        self.country = country.lower()

        if pull_more_data not in ['Yes', 'No']:
            sys.exit('Please choose whether to pull more data.')
        else:
            self.pull_more_data = pull_more_data

        available_country_list = ['argentina', 'barbados', 'brazil', 'chile',
                                    'ecuador', 'el salvador', 'greece',
                                    'mozambique', 'senegal', 'suriname', 'zambia']
        if self.country not in available_country_list:
            sys.exit('Please choose an available country.')
        self.dir = f'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet/{self.country}/'
        self.z = pd.read_csv(self.dir + f'{self.country}_fixed_income_from_list.csv')
        self.isin_list = pd.Series(self.z['isin'].unique())
        # isin_list.to_excel('zambia_isins.xlsx')
        self.isin_list = [f'/isin/{i}' for i in self.isin_list]
        self.field_list = ['ID_ISIN', 'ID_CUSIP', 'TICKER', 'Currency', 'CPN_TYP', 'BB_COMPOSITE',
                        'MATURITY', 'ISSUE_DT', 'CPN', 'YLD_YTM_BID',
                        'YLD_CUR_MID', 'PX_LAST', 'ISSUE_PX', 'REDEMP_VAL']
        self.calc_fields = ['Mty (Yrs)', 'CURR_TYPE', 'HHI']
        self.ordered_cols = ['country', 'Mty (Yrs)', 'id_isin', 'id_cusip', 'ticker', 'currency',
                                'cpn_typ', 'bb_composite', 'maturity', 'issue_dt',
                                'cpn', 'yld_ytm_bid', 'yld_cur_mid', 'px_last', 'issue_px',
                                'redemp_val', 'date', 'amt_outstanding']


    def get_data_for_isin(self, isin):
        '''
        Find relevant attributes of a given security.
        '''
        id = isin.split('/')[-1]
        fn = self.dir + f'data_{id}.csv'

        # if len([x for x in os.listdir(self.dir) if id in x]) > 0:
        #     print(f'A file for ISIN {id} already exists. Returning it.')
        #     temp = pd.read_csv(fn, index_col = 0)
        #     return temp
        # else:
        print(f'Retrieving data for ISIN {id}.')
        try:
            # find the data for the list of fields we care about.
            temp = blp.bdp(tickers = isin, flds = self.field_list)
            # create a list of the dates that the security has been active.
            date_list = pd.date_range(temp['issue_dt'][0], temp['maturity'][0], freq = 'MS')
            date_list = [datetime.datetime.strftime(x, '%Y/%m/%d') for x in date_list if x <= pd.to_datetime("today")]
            # create a list of the amt outstanding for the security for each date since issue.
            # aol = [blp.bdp(tickers = isin, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd)['amt_outstanding'][0] for dd in date_list]

            aol = Parallel(n_jobs = int(os.cpu_count()))(delayed(blp.bdp)(tickers = isin, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd) for dd in date_list)
            aol = [x['amt_outstanding'][0] for x in aol]

            # propagate the rest of the attributes through to form a dataframe.
            temp = pd.concat([temp] * len(aol), ignore_index = True)
            temp['date'] = date_list
            temp['amt_outstanding'] = aol
            temp['country'] = self.country.upper()
            temp['Mty (Yrs)'] = len(pd.date_range(end = temp['maturity'][0], start = temp['issue_dt'][0], freq = 'M')) / 12
            # save the file.
            # fn = self.dir + f'data_{id}.csv'
            temp.to_csv(fn)
            return temp
        except:
            print('Retrieval failed. Moving on.')
            pass


    def read_in_bond_data(self):
        num_existing_files = len([x for x in os.listdir(self.dir) if x.startswith('data_') and x.endswith('.csv')])
        if num_existing_files > 0:
            print('Data files already exist. Reading them in and combining them.')
            fn_list = [self.dir + f'{x}' for x in os.listdir(self.dir) if x.startswith('data_') and x.endswith('.csv')]
            # if there are only some but not all exist:
            if num_existing_files <= len(self.isin_list) and self.pull_more_data == 'Yes':
                leftover_isins = [x for x in self.z['isin'].unique() if x not in [x.split('.')[0].split('_')[1] for x in os.listdir(self.dir) if x.startswith('data_') and x.endswith('.csv')]]
                num_leftover_isins = len(leftover_isins)
                print(f'There are {num_leftover_isins} ISINs left to do.')

                # list_of_dfs = [self.get_data_for_isin(ii) for ii in tqdm(self.isin_list)]
                list_of_dfs = Parallel(n_jobs = int(os.cpu_count()))(delayed(self.get_data_for_isin)(ii) for ii in tqdm(self.isin_list))

                # concat them together with results already written to
                temp = pd.concat(list_of_dfs).reset_index(drop = True)
                out = pd.concat([pd.read_csv(x, index_col = 0) for x in fn_list])
                self.out = pd.concat([out, temp])
            else:
                self.out = pd.concat([pd.read_csv(x, index_col = 0) for x in fn_list])
            self.out = self.out[[x for x in self.ordered_cols if x in self.out.columns]].reset_index(drop = True)
            self.out.sort_values(['country', 'id_isin', 'date']).head()
        else:
            print('No data files already exist. Retrieving data now.')
            # list_of_dfs = [self.get_data_for_isin(ii) for ii in tqdm(self.isin_list)]
            list_of_dfs = Parallel(n_jobs = int(os.cpu_count()))(delayed(self.get_data_for_isin)(ii) for ii in tqdm(self.isin_list))
            self.out = pd.concat(list_of_dfs).reset_index(drop = True)
            self.out = self.out[self.ordered_cols]
            self.out = self.out.sort_values(['country', 'id_isin', 'date']).reset_index(drop = True)

        fn = self.dir + f'{self.country}_bond_data_bloomberg.csv'
        self.out.to_csv(fn)


    def calc_hhi(self):
        hhi_fn = f'{self.country}_bond_hhi_data.csv'
        if len([x for x in os.listdir(self.dir) if x == hhi_fn]) > 0:
            print('Data already exists. Moving on.')
            hhi_df = pd.read_csv(self.dir + hhi_fn, index_col = 0)
        else:
            print('Calculating the HHI for each security over time.')
            self.z = self.z[pd.notnull(self.z['report_date'])].copy().reset_index(drop = True)
            z = self.z[['isin', 'report_date', 'reported_holding']].copy()
            z['report_date'] = [datetime.datetime.strftime(pd.to_datetime(x).replace(day=1), '%Y/%m/%d') for x in z['report_date']]
            list_of_dicts = []
            for ii in tqdm(z['isin'].unique()):
                temp = z[z['isin'] == ii]
                # TODO: this does some merging on report date. I might have to do
                # some further refining to get the year and quarter, which might
                # be more accurate.
                combined = temp.merge(self.out[['id_isin', 'date', 'amt_outstanding']],
                                        left_on = ['isin', 'report_date'],
                                        right_on = ['id_isin', 'date'])
                for dd in combined['date'].unique():
                    di_temp = combined[combined['date'] == dd].copy()
                    di_temp['pct_os'] = di_temp['reported_holding'].divide(di_temp['amt_outstanding'])
                    di_temp['pct_os_sq'] = di_temp['pct_os'] ** 2
                    hhi = di_temp['pct_os_sq'].sum()
                    hhi_temp_out = dict()
                    hhi_temp_out['isin'] = ii
                    hhi_temp_out['date'] = dd
                    hhi_temp_out['hhi'] = hhi
                    list_of_dicts.append(hhi_temp_out)
            hhi_df = pd.DataFrame(list_of_dicts)
            hhi_df.to_csv(self.dir + hhi_fn)
        return hhi_df


    def main(self):
        self.read_in_bond_data()
        self.hhi_df = self.calc_hhi()
        self.hhi_df = self.hhi_df[self.hhi_df['hhi'] != np.inf]

        # for ii in hhi_df['isin'].unique():
        #     fig = px.line(hhi_df[hhi_df['isin'] == ii], x="date", y="hhi", title=f'{ii}')
        #     fig.show()

        self.new = self.out.merge(self.hhi_df,
                                    left_on = ['id_isin', 'date'],
                                    right_on = ['isin', 'date'])
        new_fn = f'{self.country}_reg_data.csv'
        if len([x for x in os.listdir(self.dir) if x == new_fn]) > 0:
            print('Regression data already exists. Moving on.')
        else:
            print('Writing out regression data.')
            self.new.to_csv(self.dir + new_fn)
        print('All done.')



if __name__ == "__main__":

    bonds = GetBondConcentrationData('Brazil', pull_more_data = 'No')
    bonds.main()


## add cusip to the existing files.
def add_cusip(fn):
    # fn = filenames[0]
    z = pd.read_csv(fn, index_col = 0)
    id = z['id_isin'].unique()[0]
    isin = f'/isin/{id}'
    cusip = blp.bdp(tickers = isin, flds = 'ID_CUSIP')
    cusip = cusip['id_cusip'].iloc[0]
    z['id_cusip'] = cusip
    z.to_csv(fn)

def fix_country(ctry):
    dir = f'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet/{ctry}/'
    filenames = [dir + fn for fn in os.listdir(dir) if fn.startswith('data_') and fn.endswith('.csv')]
    [add_cusip(fn) for fn in tqdm(filenames)]

available_country_list = ['argentina', 'barbados', 'chile',
                            'ecuador', 'el salvador', 'greece',
                            'mozambique', 'senegal', 'suriname', 'zambia']

# [fix_country(ctry) for ctry in available_country_list]


date_list = pd.date_range(temp['issue_dt'][0], temp['maturity'][0], freq = 'MS')
date_list = [datetime.datetime.strftime(x, '%Y/%m/%d') for x in date_list if x <= pd.to_datetime("today")]
date_list
# create a list of the amt outstanding for the security for each date since issue.
aol = [blp.bdp(tickers = isin, flds = ["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = dd)['amt_outstanding'][0] for dd in tqdm(date_list)]
# propagate the rest of the attributes through to form a dataframe.
temp = pd.concat([temp] * len(aol), ignore_index = True)
temp['date'] = date_list
temp['amt_outstanding'] = aol
temp['country'] = country.upper()
temp['Mty (Yrs)'] = len(pd.date_range(end = temp['maturity'][0], start = temp['issue_dt'][0], freq = 'M')) / 12
temp
# save the file.
fn = dir + f'data_{id}.csv'
temp.to_csv(fn)


isin = '/isin/ABCDEF'
isin.split('/')[1]


blp.earning('FB US Equity', Eqy_Fund_Year=2018, Number_Of_Periods=2)

###
