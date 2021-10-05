import numpy as np
import pandas as pd
import os
from os import listdir
import csv
import xlrd
import xlwt
import pyexcel as pe
# from pyexcel_xlsx import get_data
import datetime

# SimpleHistoryExample.py

import blpapi
import sys


options = blpapi.SessionOptions()
options.setServerHost('localhost')
options.setServerPort(8194)
session = blpapi.Session(options)
session.start()


SESSION_STARTED         = blpapi.Name("SessionStarted")
SESSION_STARTUP_FAILURE = blpapi.Name("SessionStartupFailure")
SERVICE_OPENED          = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE    = blpapi.Name("ServiceOpenFailure")
ERROR_INFO              = blpapi.Name("ErrorInfo")
CREATE_ORDER            = blpapi.Name("CreateOrder")


d_service="//blp/emapisvc_beta"
d_host="localhost"
d_port=8194
bEnd=False

from xbbg import blp
blp.bdh(
    ['MSFT US Equity', 'IBM US Equity', 'CSCO US Equity'],
    'Px_Last', '1/1/2014', '11/12/2014'
)

field_list = ['ID_ISIN', 'TICKER', 'Currency', 'CPN_TYP', 'BB_COMPOSITE',
                'MATURITY', 'ISSUE_DT', 'CPN', 'AMT_OUTSTANDING', 'YLD_YTM_BID',
                'YLD_CUR_MID', 'PX_LAST', 'ISSUE_PX', 'REDEMP_VAL']
calc_fields = ['Mty (Yrs)', 'CURR_TYPE', 'HHI']

blp.bdp(tickers="JK424419@CBBT Corp", flds=["ISSUE_DT"])
blp.bdp(tickers="JK424419@CBBT Corp", flds=["MATURITY"])
blp.bdp(tickers="JK424419@CBBT Corp", flds=["MTY_YEARS_TDY"])

blp.bdp(tickers="JK424419@CBBT Corp",
        flds=["AMT_OUTSTANDING"], AMOUNT_OUTSTANDING_AS_OF_DT = '2018/01/01')

options = blpapi.SessionOptions()
options.setServerHost('localhost')
options.setServerPort(8194)
session = blpapi.Session(options)
session.start()
session.openServiceAsync(d_service)


from optparse import OptionParser
import tia.bbg.datamgr as dm

mgr = dm.BbgDataManager()
sids = mgr['MSFT US EQUITY', 'IBM US EQUITY', 'CSCO US EQUITY']
df = sids.get_historical('PX_LAST', '1/1/2014', '11/12/2014')

import pdblp
con = pdblp.BCon(debug = True, port = 8194, timeout = 5000)
con.start()
con.bdh(['IBM US Equity', 'MSFT US Equity'], ['PX_LAST', 'OPEN'],
        '20061227', '20061231', elms=[("periodicityAdjustment", "ACTUAL")])

## ticket number H#1157380640

ticker = 'AAPL US EQUITY'
sids = mgr[ticker]

sids = mgr['BAC US EQUITY', 'JPM US EQUITY']

br_bonds = ['EF238767@CBBT CORP', 'ED289312@CBBT CORP',
            'EC232402@CBBT CORP', 'EI224092@CBBT CORP']

info_df = sids.get_historical(
       ['BEST_SALES','BEST_OPP', 'BEST_EBITDA', 'BEST_EBIT'],
       start="1/1/2000",
       end="6/30/2016",
       period="DAILY",
       BE997="1GY")

df = sids.get_historical(['BEST_PX_BPS_RATIO','BEST_ROE'],
                         datetime.date(2013,1,1),
                         datetime.date(2013,2,1),
                         BEST_FPERIOD_OVERRIDE="1GY",
                         non_trading_day_fill_option="ALL_CALENDAR_DAYS",
                         non_trading_day_fill_method="PREVIOUS_VALUE")

#and you'll probably want to carry on with something like this
df1=df.unstack(level=0).reset_index()
df1.columns = ('ticker','field','date','value')
df1.pivot_table(index=['date','ticker'],values='value',columns='field')
df1.pivot_table(index=['date','field'],values='value',columns='ticker')



def parseCmdLine():
    parser = OptionParser(description="Retrieve reference data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)

    (options, args) = parser.parse_args()

    return options


def main():
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    print "Connecting to %s:%s" % (options.host, options.port)
    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        print "Failed to start session."
        return

    try:
        # Open service to get historical data from
        if not session.openService("//blp/refdata"):
            print "Failed to open //blp/refdata"
            return

        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")

        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")
        request.getElement("securities").appendValue("IBM US Equity")
        request.getElement("securities").appendValue("MSFT US Equity")
        request.getElement("fields").appendValue("PX_LAST")
        request.getElement("fields").appendValue("OPEN")
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", "DAILY")
        request.set("startDate", "20061227")
        request.set("endDate", "20061231")
        request.set("maxDataPoints", 100)

        print "Sending Request:", request
        # Send the request
        session.sendRequest(request)

        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                print msg

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
    finally:
        # Stop the session
        session.stop()

if __name__ == "__main__":
    print "SimpleHistoryExample"
    try:
        main()
    except KeyboardInterrupt:
        print "Ctrl+C pressed. Stopping..."
