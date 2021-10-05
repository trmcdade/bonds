import pandas as pd
import numpy as np
import datetime as dt
import sys
import os
from os import listdir
from tqdm import tqdm
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
from joblib import Parallel, delayed


class FindVolatility:


    def __init__(self, fn, pull_date):
        """
        Author: Timothy R. McDade
        Date: 1 Sept 2021.

        This code pulls trade-level price data for a particular municipal debt
        security and calculates  rolling historical price volatility.

        The steps to do so are as follows:
        1. Import and clean the data.
        2. Disaggregate the data by finding the closing price of each trading day.
        3. Calculate the rolling hisotrical volatility: 10d, 30d, 60d, 90d.
        4. Graph the trajectory, if you want.
        5. Output the file.
        """
        self.dir = f'C:/Users/trmcd/OneDrive/Duke/Papers/Bonds/Data/Bloomberg/California/'
        self.fn = fn
        self.pull_date = pull_date
        self.cores = int(os.cpu_count())


    def find_close_px_and_yd(self, dd):
        '''
        Find the price and yield at close for each day.
        TODO: delete this function, no longer needed. Moved its contents to
        import_and_clean_data().
        '''
        temp = self.p[self.p['DATE'] == dd].copy()
        temp = temp.sort_values('DATETIME', ascending = True)
        px, yd = temp[['PRICE', 'YIELD']].iloc[-1]
        return px, yd


    def import_and_clean_data(self):
        '''
        Pull in the data from MSRB, via WRDS.
        CA muni bond prices 2013-present, the same time period as holdings data,
        sourced from MSRB, via the below links:
        https://wrds-www.wharton.upenn.edu/pages/get-data/msrb-municipal-securities-rulemaking-board/municipal-securities-transaction-data/
        https://www.msrb.org/Market-Transparency/Subscription-Services-and-Products/MSRB-Transaction-Subscription/Academic-Historical-Data-Product-Format
        '''
        print('Importing the data.')
        p = pd.read_csv(self.dir + f'{self.fn}.csv', low_memory = False)
        keep_cols = ['CUSIP', 'TRADE_DATE', 'TIME_OF_TRADE',
                     'PAR_TRADED', 'DOLLAR_PRICE', 'YIELD']
        p = p[keep_cols].sort_values(['CUSIP', 'TRADE_DATE', 'TIME_OF_TRADE'], ascending = True)
        p = p.rename(columns = {'TRADE_DATE':'DATE', 'TIME_OF_TRADE':'TIME',
                                'DOLLAR_PRICE':'PRICE', 'PAR_TRADED':'PAR_VALUE'})
        p['DATE'] = p['DATE'].astype('str')
        p['DATETIME'] = p['DATE'] + ' ' + p['TIME']
        p['DATETIME'] = pd.to_datetime(p['DATETIME'], format = '%Y-%m-%d %H:%M:%S')
        p = p.sort_values(['CUSIP', 'DATETIME'], ascending = True)

        # get the price and yield on close.
        print('Calculating close price and yield.')
        close_px_df = p[['CUSIP', 'DATE', 'PRICE']].groupby(['CUSIP', 'DATE']).last().reset_index()
        close_px_df = close_px_df.rename(columns = {'PRICE':'close_px'})
        close_yd_df = p[['CUSIP', 'DATE', 'YIELD']].groupby(['CUSIP', 'DATE']).last().reset_index()
        close_yd_df = close_yd_df.rename(columns = {'YIELD':'close_yd'})

        p = p.merge(close_px_df, how = 'inner', on = ['CUSIP', 'DATE'])
        p = p.merge(close_yd_df, how = 'inner', on = ['CUSIP', 'DATE'])

        # make it daily rather than trade-level data.
        p = p[['CUSIP', 'DATE', 'close_px', 'close_yd']].drop_duplicates().sort_values('DATE').reset_index(drop = True)
        p['DATE'] = pd.to_datetime(p['DATE'], format = '%Y%m%d')

        return p


    def calculate_volatility(self, data, num_days_back):
        '''
        This function returns the annualized stdev of the logarithmic daily
        price changes for the xyz recent trading days closing price, expressed
        as a pct. The only difference between them and me is that I don't
        express mine as a pct. They don't say as a pct of what.

        From BBG Help desk:
        The definition of the VOLATILITY_xxD fields are the standard deviation
        of the daily differences in natural log of the prices, and multiply by
        sqrt(260) to annualise.
        The following BQL formula serves well to show how it's done from scratch:
        =BQL("13063A5G Muni","std(last(diff(ln(dropna(px_last(dates=range(2014-01-28-30d,2014-01-28))))),9))*sqrt(260)*100")
        '''
        # print(f'Calculating historical {num_days_back}D volatility.')
        num_days = 1 + num_days_back # start and end date inclusive, so it should be 1 plus whatever.
        # the log daily price change.
        daily_returns = np.log(data['close_px'] / data['close_px'].shift(1))
        daily_returns.fillna(0, inplace = True)
        # take std and annualize it
        vol = daily_returns.rolling(window = num_days).std() * np.sqrt(252)
        return daily_returns, vol


    def find_volatility(self, cusip):

        '''
        Fill in the volatility of different periods for one CUSIP.
        '''
        temp = self.p[self.p['CUSIP'] == cusip]
        # Uncomment the last bit here to express the volatility as a
        # percentage of the price. Not sure it is necessary.
        temp.loc[:,'log_ret'] = self.calculate_volatility(temp, 3)[0] #/ temp.loc[:,'close_px']
        temp.loc[:,'vol_3d'] = self.calculate_volatility(temp, 3)[1] #/ temp.loc[:,'close_px']
        temp.loc[:,'vol_5d'] = self.calculate_volatility(temp, 5)[1] #/ temp.loc[:,'close_px']
        temp.loc[:,'vol_10d'] = self.calculate_volatility(temp, 10)[1] #/ temp.loc[:,'close_px']
        temp.loc[:,'vol_30d'] = self.calculate_volatility(temp, 30)[1] #/ temp.loc[:,'close_px']
        temp.loc[:,'vol_60d'] = self.calculate_volatility(temp, 60)[1] #/ temp.loc[:,'close_px']
        temp.loc[:,'vol_90d'] = self.calculate_volatility(temp, 90)[1] #/ temp.loc[:,'close_px']

        return temp


    def graph_volatilities(self, data):
        '''
        Plot all the volatilities on the same graph. Formatted to look
        like a Bloomberg graph.
        '''
        id = data['CUSIP'].unique()[0]
        data = data.sort_values('DATE')
        # formatter = mdates.DateFormatter("%Y") ### formatter of the date
        # locator = mdates.YearLocator() ### where to put the labels

        plt.rcParams.update({
            "lines.color": "white",
            "patch.edgecolor": "white",
            "text.color": "black",
            "axes.facecolor": "black",
            "axes.edgecolor": "lightgray",
            "axes.labelcolor": "white",
            "xtick.color": "white",
            "ytick.color": "white",
            "grid.color": "lightgray",
            "figure.facecolor": "black",
            "figure.edgecolor": "black",
            "savefig.facecolor": "black",
            "savefig.edgecolor": "black"})

        fig = plt.figure(figsize=(15, 7))
        ax1 = fig.add_subplot(1, 1, 1)
        d3, = ax1.plot(data['DATE'], data['vol_3d'], color = 'yellow')
        d5, = ax1.plot(data['DATE'], data['vol_5d'], color = 'red')
        d10, = ax1.plot(data['DATE'], data['vol_10d'], color = 'lightblue')
        d30, = ax1.plot(data['DATE'], data['vol_30d'], color = 'green')
        d60, = ax1.plot(data['DATE'], data['vol_60d'], color = 'orange')
        d90, = ax1.plot(data['DATE'], data['vol_90d'], color = 'pink')
        ax2 = ax1.twinx()
        px, = ax2.plot(data['DATE'], data['close_px'], color = 'white')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Volatility')
        ax2.set_ylabel('Closing Price')
        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
        ax1.xaxis.set_minor_locator(mdates.MonthLocator())
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        leg = plt.legend([d3, d5, d10, d30, d60, d90, px], ['VOL_3D', 'VOL_5D', 'VOL_10D', 'VOL_30D', 'VOL_60D', 'VOL_90D', 'PRICE'])
        for text in leg.get_texts():
            text.set_color("white")
        fig.suptitle(f'Historical Volatility for CUSIP {id}', color = 'white')
        plt.gcf().autofmt_xdate()
        plt.show()


    def main(self):
        '''
        The main function runs the processes.
        '''
        print('Process initiated.')
        self.p = self.import_and_clean_data()
        print('Getting volatilities.')
        file_list = [x for x in os.listdir(self.dir) if '_px_vol.csv' in x and 'all_bonds_' not in x]
        for cusip in tqdm(self.p['CUSIP'].unique()):
            temp_out_fn = f'{cusip}_px_vol.csv'
            # If you want to rely on the existing data rather than re-running
            # the whole thing, uncomment the below if clause and indent the
            # following lines, until and including the temp.to_csv(...) line.
            # if temp_out_fn not in file_list:
            temp = self.find_volatility(cusip)
            # self.graph_volatilities(temp) # uncomment to see each graph
            temp.to_csv(self.dir + temp_out_fn)
        self.out_df = pd.concat([pd.read_csv(self.dir + ff, index_col = 0) for ff in file_list], axis = 0).reset_index(drop = True)
        # write it all out.
        out_fn = f'all_bonds_px_vol_{self.pull_date}.csv'
        self.out_df.to_csv(self.dir + out_fn)

        print('All done.')


if __name__ == "__main__":
    fv = FindVolatility('cali_muni_since_2005_prices',
                            pull_date = '2021-09-30')
    fv.main()

fv.graph_volatilities(fv.out_df[fv.out_df['CUSIP'] == '13063A5G5'])

#
