from tqdm import tqdm # useful little package for creating progress bars
import re # for regular expressions
import pandas as pd
import numpy as np
import os
os.chdir("C:\\Users\\trmcdade\\OneDrive\\Laptop\\blpapi_cpp_3.12.3.1")

import blpapi
import pdblp
import datetime
import tia.bbg.datamgr as dm
mgr = dm.BbgDataManager()
sids = mgr['BAC US EQUITY', 'JPM US EQUITY']
df = sids.get_historical(['BEST_PX_BPS_RATIO','BEST_ROE'],
                         datetime.date(2013,1,1),
                         datetime.date(2013,2,1),
                         BEST_FPERIOD_OVERRIDE="1GY",
                         non_trading_day_fill_option="ALL_CALENDAR_DAYS",
                         non_trading_day_fill_method="PREVIOUS_VALUE")
print(df)

# Single Security/Field
blp.referenceRequest('BBD/B CN Equity', 'GICS_SECTOR')

# Multiple Securities/Fields
blp.referenceRequest(['CNR CN Equity', 'CP CN Equity'], ['SECURITY_NAME_REALTIME', 'LAST_PRICE']))

# Basic Historical Request
blp.historicalRequest('BMO CN Equity', 'PX_LAST', '20141231', '20150131')

# Multiple Fields, Dates as datetime
blp.historicalRequest('BNS CN Equity', ['PX_LAST', 'PX_VOLUME'], datetime(2014, 12, 31), datetime(2015, 1, 31)))

# Multiple Securities/Fields
blp.historicalRequest(['CM CN Equity', 'NA CN Equity'], ['PX_LAST', 'PX_VOLUME'], '20141231', '20150131')

# Arbitrary keyword arguments are included in the request
blp.historicalRequest('TD CN Equity', 'PCT_CHG_INSIDER_HOLDINGS', '20141231', '20150131', periodicitySelection='WEEKLY')

# Bulk Data Requests
blp.bulkRequest('CP CN Equity','PG_REVENUE'))



from xbbg import blp
blp.bdh(
    ['MSFT US Equity', 'IBM US Equity', 'CSCO US Equity'],
    'Px_Last', '1/1/2014', '11/12/2014'
)
con = pdblp.BCon(debug=True, port=8194, timeout=5000)

con.start()

con.bsrch("COMDTY:VESSEL").head()

# this is in Java but it provides a search algorithm for filtering
# govt securities
# Service govtService = session.getService("//blp/instruments");
# Request request = govtService.createRequest("govtListRequest");
# request.asElement().setElement("partialMatch", "true");#
# request.asElement().setElement("query", "T*");
# request.asElement().setElement("ticker", "LANG_OVERRIDE_NONE");
# request.asElement().setElement("maxResults", "10");
#sendRequest(request, session)

# Parsekeyable:
# EK709657@BVAL Govt
# //blp/mktdata/ticker/EK709657@BVAL Govt
# BBGID:
# BBG007Z1JW11@BVAL Govt
# //blp/mktdata/bbgid/BBG007Z1JW11@BVAL
# ISIN:
# US38148LAC00@BVAL Govt
# //blp/mktdata/isin/US38148LAC00@BVAL
# CUSIP:
# 38148LAC@BVAL Govt
# //blp/mktdata/cusip/38148LAC@BVAL
# SEDOL:
# BVGCLY7@BVAL Govt
# //blp/mktdata/sedol/BVGCLY7@BVAL

# Creating a Parsekeyable for a: 10-Year US Treasury, maturing in May 2024, coupon 2.5, source BVAL
# Issuer x Coupon x Maturity x Provider x Yellow Key T 2.5 05/15/24 @BVAL Govt

# print(instrreq.asElement().elementDefinition().toString())
# instrreq.yellowKeyFilter == 'YK_FILTER_GOVT'
# instrreq.yellowKeyFilter == 'YK_FILTER_MUNI'
# WB (World Bond Markets)
# Government Securities "GOVT"
# For each bond, "FLDS PARSE"

con.bdh('SPY US Equity', 'PX_LAST',
                '20150629', '20150630')
DEBUG:root:Sending Request:
 HistoricalDataRequest = {
    securities[] = {
        "SPY US Equity"
    }
    fields[] = {
        "PX_LAST"
    }
    periodicityAdjustment = ACTUAL
    periodicitySelection = DAILY
    startDate = "20150629"
    endDate = "20150630"
    overrides[] = {
    }
}
DEBUG:root:Message Received:
 HistoricalDataResponse = {
    securityData = {
        security = "SPY US Equity"
        eidData[] = {
        }
        sequenceNumber = 0
        fieldExceptions[] = {
        }
        fieldData[] = {
            fieldData = {
                date = 2015-06-29
                PX_LAST = 205.420000
            }
            fieldData = {
                date = 2015-06-30
                PX_LAST = 205.850000
            }
        }
    }
}
