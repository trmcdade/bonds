import pandas as pd
import os
from os import listdir
from tqdm import tqdm
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

dir = 'C:/Users/trmcd/OneDrive/Duke/Papers/Bonds/Data/Bloomberg/California/'
r = pd.read_csv(dir + 'cali_cusip_list.csv', index_col = 0)
# The number of bonds in the original pull is:
r.shape[0]

# The number of bonds in the pricing data is:
prices = pd.read_csv(dir + 'all_bonds_px_vol_2021-09-30.csv', index_col = 0)
len(prices['CUSIP'].unique())

# The number of bonds in the holdings data is:
h_filenames = [dir + f'../../FactSet/SQL/California/{x}' for x in os.listdir(dir + '../../FactSet/SQL/California/') if '_holdings.csv' in x and 'combined' not in x]
holdings = pd.concat([pd.read_csv(x, index_col = 0) for x in tqdm(h_filenames)])
len(holdings['isin'].unique())

g = pd.read_csv(dir + f'../../FactSet/CA_reg_data_2021-09-30.csv', index_col = 0)
len(g['CUSIP'].unique())
g = g.sort_values(['CUSIP', 'date'])
len(g['CUSIP'].unique())
len(g[(pd.notna(g['hhi']))]['CUSIP'].unique())
len(g[(g['hhi'] <= 1) & (g['hhi'] >= 0)]['CUSIP'].unique())
len(g[(g['pct_os_known'] <= 1) & (g['pct_os_known'] >= 0)]['CUSIP'].unique())
g = g[(pd.notna(g['hhi'])) & (g['hhi'] <= 1) & (g['pct_os_known'] <= 1)]

# Construct a data frame that gets the unique number of hhis and dates per security.
keep_cusips = list()
n_hhi = list()
n_dates = list()
ordered_cusips = g.sort_values('amt_outstanding', ascending = False)['CUSIP'].drop_duplicates()
for cc in tqdm(ordered_cusips):
  temp = g[g['CUSIP'] == cc]
  if len(temp['hhi'].unique()) > 1:
    keep_cusips.append(cc)
    n_hhi.append(len(temp['hhi'].unique()))
    n_dates.append(len(temp['date_q'].unique()))

diagnostics = pd.DataFrame([keep_cusips, n_hhi, n_dates], index = ['CUSIP', 'n_hhi', 'n_dates']).T
diagnostics = diagnostics.sort_values(['n_dates', 'n_hhi'], ascending = [False, False])
diagnostics = diagnostics.merge(g[['CUSIP', 'Mty (Yrs)']].drop_duplicates(), on = 'CUSIP')
diagnostics.head()

# Plot the distribution of hhis and time series available.
sns.set_style('whitegrid')
h = sns.jointplot(data = diagnostics,
                    x = n_hhi, y = n_dates,
                    hue = 'Mty (Yrs)',
                    height = 8, ratio = 8,
                    xlim = (-1,  diagnostics['n_dates'].max() + 1),
                    ylim = (-10, diagnostics['n_hhi'].max() + 1)
                    )
plt.subplots_adjust(top=0.9)
h.set_axis_labels('Number of Unique HHI per CUSIP', 'Number of Quarters with Holdings Data Available')
# g.fig.suptitle(f'Protest Count by Social Insurance Level')
plt.tight_layout()
figname = bonds.dir + f'../../LaTeX/description_hhi_dates.png'
plt.savefig(figname)
