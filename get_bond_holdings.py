from tqdm import tqdm
import pyodbc
import pandas as pd
import numpy as np
import os, time
import sys
from joblib import Parallel, delayed


class PullHoldingData:


    def __init__(self):
        '''
        Author: Timothy R. McDade
        Date: 20 Sept 2021

        This code takes a long time to run, considering that it has to
        retrieve data for ~4600 ISINs.

        The general work flow is:
        1. Pull the distinct CUSIPs from the bond information data.
        2. Pull the holdings info for each CUSIP from the FactSet data base
           separately.
        3. Concatenate them all together and write out a master file.
        '''
        self.dir = os.getcwd().replace('\\', '/') + '/../Data/FactSet/'
        credentials = open(self.dir + 'credentials.txt', 'r')
        c = credentials.read()
        c = c.split('\n')
        server = c[0].split('= ')[1].replace("'", "")
        database = c[1].split('= ')[1].replace("'", "")
        username = c[2].split('= ')[1].replace("'", "")
        password = c[3].split('= ')[1].replace("'", "")
        self.cnxn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')


    def get_cusip_list(self):
        cusip_query = "SELECT DISTINCT CUSIP FROM dbo.cali_cusips"
        cusip_list = pd.read_sql(cusip_query, self.cnxn)
        self.cusip_list = [x for x in cusip_list['CUSIP']]


    def pull_holding_data(self, cusip):
        query = '''
                --DECLARE @cusip VARCHAR(16);
                --SET @cusip = '{cusip}'; --CUSIP of the security, i.e., AAPL-US
                --SET @cusip = '13063A5G5' --A sample CA Muni.

                --Most Recent Top Institutional Holders
                select top 10 --*
                --SELECT DISTINCT
                		d.fsym_id
                		,i.isin
                		,c.cusip

                		,cov.proper_name
                		,osc.security_name

                		,cov.currency
                		,cov.active_flag
                		,osc.active
                		,cov.fref_security_type
                		,cov.universe_type

                		,osc.issue_type
                		,osc.iso_country AS 'Security Country'

                		,inst.factset_entity_id AS 'Managing Inst. Id'
                		,inst.entity_proper_name AS 'Managing Inst.'
                		,inst.iso_country AS 'Managing Inst. Country'
                		,inst.entity_type

                		,d.report_date

                		,SUM(adj_holding) OVER(PARTITION BY inst.factset_entity_id, d.report_date) AS 'Managing Inst. Adj. Holding'
                		,SUM(adj_mv) OVER(PARTITION BY inst.factset_entity_id, d.report_date) AS 'Managing Inst. Adj. MV'
                		,SUM(reported_holding) OVER(PARTITION BY inst.factset_entity_id, d.report_date) AS 'Managing Inst. Reported Holding'
                		,SUM(reported_mv) OVER(PARTITION BY inst.factset_entity_id, d.report_date) AS 'Managing Inst. Reported MV'

                		--,filer.factset_entity_id AS 'Filer Id'
                		--,filer.entity_proper_name AS 'Filer Name'
                		--,filer.entity_type
                		--,d.adj_holding
                		--,d.adj_mv / 1000000 AS 'Adj MV (Millions)'
                		--,d.reported_holding
                		--,d.reported_mv / 1000000 AS 'Reported MV (Millions)'

                FROM sym_v1.sym_coverage AS cov
                JOIN sym_v1.sym_isin i on cov.fsym_security_id = i.fsym_id --get the isin.
                JOIN sym_v1.sym_cusip c on cov.fsym_security_id = c.fsym_id --get the cusip.

                JOIN own_v5.own_sec_coverage osc on osc.fsym_id = cov.fsym_security_id

                -- TODO: Do I need to incorporate [own_v5].[own_ent_funds_feeder_master]? Probably, somehow.

                -- TODO: Ideally, I'd be able to roll it up even further: to "Fidelity", "BlackRock", "The Vanguard Group".

                JOIN own_v5.own_fund_detail AS d ON d.fsym_id = cov.fsym_security_id --what entities own the security
                JOIN own_v5.own_ent_funds AS oef on oef.factset_fund_id = d.factset_fund_id --technicals of the managing institution
                JOIN own_v5.own_ent_institutions AS oei ON oei.factset_entity_id = oef.factset_inst_entity_id --info about the subsector of finance and the aum etc

                JOIN sym_v1.sym_entity AS inst ON inst.factset_entity_id = oef.factset_inst_entity_id --name and id of the parent institution
                JOIN sym_v1.sym_entity AS filer ON filer.factset_entity_id = oef.factset_fund_id --name and id of the filing fund

                WHERE 1=1
                AND c.cusip = ?
                --AND c.cusip = '13063A5G5' --A sample CA Muni.
                --AND inst.factset_entity_id = '000KLZ-E' --Fidelity as Managing Inst.
                --AND (c.cusip in (SELECT DISTINCT CUSIP FROM dbo.cali_cusips)) --to get the data for all the CUSIPs.

                ORDER BY d.report_date DESC
                --, d.reported_holding DESC
                ,[Managing Inst. Reported Holding] DESC;
                '''

        temp_fn = self.dir + f'/SQL/California/{cusip}_holdings.csv'
        temp = pd.read_sql(query, self.cnxn, params = [cusip])
        if temp.shape[0] == 0:
            temp.loc[0,'CUSIP'] = cusip
        temp.to_csv(temp_fn)
        return temp


    def main(self):
        print('Get list of CUSIPs.')
        self.get_cusip_list()
        # so we don't double pull.
        files_already_retrieved = [x for x in os.listdir(self.dir + 'SQL/California/') if '_holdings.csv' in x and 'combined' not in x]
        cusips_already_gotten = [x.split('_holding')[0] for x in files_already_retrieved]
        self.cusip_list = [x for x in h.cusip_list if x not in cusips_already_gotten]

        # pull the data.
        print('Pulling holding data for all CUSIPs.')
        self.holdings_list = [self.pull_holding_data(cusip) for cusip in tqdm(self.cusip_list)]
        # holdings_list = Parallel(n_jobs=int(os.cpu_count()))(delayed(pull_holding_data)(cusip) for cusip in tqdm(cusip_list))

        # combine all files.
        print('Combining all output.')
        all_files_list = [x for x in os.listdir(self.dir + 'SQL/California/') if '_holdings.csv' in x and 'combined' not in x]
        all_holdings_list = [pd.read_csv(self.dir + 'SQL/California/' + ff, index_col = 0) for ff in all_files_list]
        self.holdings_df = pd.concat(all_holdings_list).reset_index(drop = True)

        out_fn = self.dir + f'/SQL/combined_holdings.csv'
        self.holdings_df.to_csv(out_fn)
        print('All done.')



if __name__ == "__main__":

    h = PullHoldingData()
    h.main()

#
