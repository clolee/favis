#!/usr/bin/python
import requests
import datetime
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
DB_STOCK_DAILY_INFO = '../db/stock_daily_sellbuy_trend.db'
    
conn = sqlite3.connect(DB_STOCK_MASTER)
cur = conn.cursor()
df_sm = pd.read_sql_query('SELECT * FROM stock_master', conn)
if conn:
    conn.close()
    
conn = sqlite3.connect(DB_STOCK_DAILY_INFO)

#day = datetime.datetime.today().strftime('%Y%m%d')
day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')
print(day)
for idx, row in df_sm.iterrows():
	stock_code = row['code']
	stock_name = row['name']

	isu_cd = util.getIsinCode(stock_code)

	path = "./data/"
	filename = path + stock_code + "_sellbuy_trend.xls"
	r = util.get_krx_daily_sellbuy_trend(isu_cd, day, day)
	with open(filename, 'wb') as f:
		f.write(r)

	df = pd.read_excel(filename, thousands=',', usecols=['년/월/일', '종가','대비','거래량(주)','기관_순매수(주)','외국인_순매수(주)'])
	df = df.dropna()
	df.columns = ['date','close','rate','volume','i_volume', 'f_volume']
	df['date'] = df['date'].str.replace('/','')
	df_cp = df[['date','close','rate','volume','i_volume', 'f_volume']].copy()
	df_cp['stock_code'] = stock_code
        
	df_cp = df_cp[['stock_code', 'date','close','rate','volume','i_volume', 'f_volume']]

	try:
		with conn:
			df_cp.to_sql('stock_daily_sellbuy_trend', conn, index=False, if_exists='append')
	except sqlite3.IntegrityError:
		pass
	except sqlite3.Error as e:
		if conn:
			conn.rollback()
		print ("error %s" % e.args[0])

print(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
if conn:
    conn.close()
