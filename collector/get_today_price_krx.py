#!/usr/bin/python
import requests
import datetime, time
import pandas as pd
from pandas import DataFrame
import io, os
# user define package import
import sys
sys.path.append("../../favis")
from msgbot.favisbot import favisbot
import util.krx_util as util
import sqlite3

DB_STOCK_MASTER = '../db/stock_master.db'
DB_STOCK_DAILY_INFO = '../db/stock_daily_info.db'
    
# main

conn = sqlite3.connect(DB_STOCK_MASTER)
cur = conn.cursor()
df_sm = pd.read_sql_query('SELECT * FROM stock_master', conn)
if conn:
    conn.close()


conn = sqlite3.connect(DB_STOCK_DAILY_INFO)
cur = conn.cursor()
	   

print (datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))

#today = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d');
today = '20160527'
for idx, row in df_sm.iterrows():
	stock_code = row['code']
	stock_name = row['name']
    #print (row['code'], util.getIsinCode(row['code']))

	isu_cd = util.getIsinCode(row['code'])

	path = "./data/"
	filename = path + stock_code + "_" + today + ".xls"
	if not os.path.exists(filename):
		r = util.get_krx_daily_info(isu_cd, today, today)
		with open(filename, 'wb') as f:
			#f.write(r.content.decode('utf-8'))
			f.write(r)

	df = pd.read_excel(filename, thousands=',', usecols=['년/월/일', '종가','거래량(주)','시가','고가','저가', '시가총액(백만)','상장주식수(주)'])

	df.columns = ['date','close','volume','open','high','low', 'marcap','amount']
	df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
	df_cp = df[['date','close','volume','open','high','low', 'marcap','amount']].copy()
	df_cp['stock_code'] = stock_code

	
	df_cp = df_cp[['stock_code', 'date', 'open', 'high', 'low', 'close', 'volume', 'marcap', 'amount']]
	print(df_cp.head())
	#df.set_index('Date', inplace=True)
	#df = df.sort_index(0, ascending=True)
	#print(df_cp[['stock_code', 'Date','Open','High','Low','Close','Volume','marcap','amount']].head())

	try:
		with conn:
			cur = conn.cursor()
			cur.execute('CREATE TABLE if not exists stock_daily_info ( stock_code, date, open, high, low, close, volume, marcap, amount,regdate DATETIME DEFAULT CURRENT_TIMESTAMP)')
			cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_info ON stock_daily_info (stock_code, date)')

	#         cur.executemany("INSERT OR REPLACE INTO  stock_daily_info (stock_code, date, open, high, low, close, volume, marcap, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", datalist)
	#         conn.commit()
	except sqlite3.Error as e:
		if conn:
			conn.rollback()
		print ("error %s" % e.args[0])

if conn:
    conn.close()