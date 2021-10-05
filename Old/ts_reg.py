import pandas as pd
import numpy as np
from patsy import dmatrices
import statsmodels.api as sm
from statsmodels.tsa.api import VAR
from statsmodels.tsa.base.datetools import dates_from_str

dir = 'C:/Users/trmcd/OneDrive/Duke/Papers/Summer 2019 Proposal/Data/FactSet/brazil/'
df = pd.read_csv(dir + 'brazil_reg_data.csv', index_col = 0)
df.columns = [x.replace('.','') for x in df.columns]

def get_quarter_diff(start, end):
    t = pd.to_datetime(end).to_period(freq='Q') - pd.to_datetime(start).to_period(freq='Q')
    return(t.n)

df['qtm'] = [get_quarter_diff(df['refdate'].iloc[ii], df['maturdate'].iloc[ii]) for ii in range(df.shape[0])]

df = df[['ISIN', 'psd', 'mat_yr', 'pricebid', 'hhi', 'qtm', 'date', 'yyQq']].sort_values(['ISIN', 'yyQq']).reset_index(drop = True)
df = df.groupby(['ISIN', 'psd', 'mat_yr', 'hhi', 'qtm', 'date', 'yyQq']).mean().reset_index().sort_values(['ISIN', 'yyQq'])

def get_longest_isin(ii):
    # ii = df['ISIN'].unique()[0]
    temp = df[df['ISIN'] == ii]
    return (ii, temp.shape[0])

# [get_longest_isin(ii) for ii in df['ISIN'].unique()]

df = df[df['ISIN'] == 'BRSTNCNTB0O7']

# create multi-index with isin and yyqq as ids.
# index_cols = [np.array(df['ISIN']), np.array(pd.PeriodIndex(pd.to_datetime(df['date']), freq='Q').to_timestamp())]
# idx = pd.MultiIndex.from_arrays(index_cols, names = ['ISIN', 'yyQq'])
# df.index = idx
# df = df.drop(['ISIN', 'date', 'yyQq'], axis = 1)

# this works for the time index of just one ac.
quarterly = pd.PeriodIndex(pd.to_datetime(df['date']), freq='Q').to_timestamp()
df.index = quarterly
df = df.drop(['ISIN', 'yyQq', 'date'], axis = 1)

# VAR(p) processes, only appropriate if stationary.
data = np.log(df).diff().dropna()
model = VAR(data)
results = model.fit(2)
results.summary()
