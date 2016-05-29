#!/usr/bin/python
import requests
import datetime
import pandas as pd
from pandas import DataFrame
import io
# user define package import
import sys
sys.path.append("../../favis")
from msgbot.favisbot import favisbot
import util.krx_util as util
import sqlite3

DB_STOCK_MASTER = '../db/stock_master.db'
DB_STOCK_DAILY_INFO = '../db/stock_daily_info.db'
    
# main

# conn = sqlite3.connect(DB_STOCK_MASTER)
# cur = conn.cursor()

# df_sm = pd.read_sql_query('SELECT * FROM stock_master', conn)

# for idx, row in df_sm.iterrows():
stock_code = '014160'
#stock_code = row['code']
#stock_name = row['name']
#print (row['code'], util.getIsinCode(row['code']))

yesterday = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')
print(yesterday)
#print(stock_code, stock_name, year)

isu_cd = util.getIsinCode(stock_code)

r = util.get_krx_daily_price(isu_cd, yesterday, yesterday, 'csv')

df = pd.read_csv(io.StringIO(r.decode("utf-8")), thousands=',' , usecols=['﻿﻿년/월/일', '종가','거래량(주)','시가','고가','저가', '시가총액(백만)','상장주식수(주)'])

df.columns = ['date','close','volume','open','high','low', 'marcap','amount']
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
df_cp = df[['date','close','volume','open','high','low', 'marcap','amount']].copy()
df_cp['stock_code'] = stock_code


df_cp = df_cp[['stock_code', 'date', 'open', 'high', 'low', 'close', 'volume', 'marcap', 'amount']]

#df_cp.to_sql('stock_daily_info', conn, index=False, if_exists='replace')

#df = pd.read_sql_query('SELECT * FROM stock_daily_info limit 5', conn)
print(df_cp.head())

# if conn:
#     conn.close()

