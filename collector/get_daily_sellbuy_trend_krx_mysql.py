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
import util.favis_util as favis
import pymysql

conn = favis.get_favis_mysql_connection()
cur = conn.cursor()
df_sm = pd.read_sql_query('SELECT * FROM stock_info', conn)

print (datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
for idx, row in df_sm.iterrows():
	stock_code = row['code']
	stock_name = row['name']
	#print (row['code'], util.getIsinCode(row['code']))

	year_start = 2010
	year_end = 2016
	for year in range(year_start, year_end + 1):
		print(stock_code, stock_name, year)
		start = datetime.datetime(year, 1, 1).strftime('%Y%m%d')
		end = datetime.datetime(year, 12, 31).strftime('%Y%m%d')

		isu_cd = util.getIsinCode(stock_code)

		path = "./data/"
		filename = path + stock_code + "_" + start + "-" + end + "-trend.xls"
		if not os.path.exists(filename):
			r = util.get_krx_daily_sellbuy_trend(isu_cd, start, end)
			with open(filename, 'wb') as f:
				#f.write(r.content.decode('utf-8'))
				f.write(r)


		#df = pd.read_excel(filename, thousands=',', usecols=['년/월/일', '종가','대비','거래량(주)','기관_순매수(주)','외국인_순매수(주)'])
		df = pd.read_excel(filename, thousands=',', usecols=['년/월/일', '기관_순매수(주)','외국인_순매수(주)'])
		df = df.dropna()
		df.columns = ['date','i_volume', 'f_volume']
		#        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
		df['date'] = df['date'].str.replace('/','')
		df_cp = df[['date','i_volume', 'f_volume']].copy()
		df_cp['stock_code'] = stock_code
		#        print(df_cp)

		df_cp = df_cp[['stock_code', 'date','i_volume', 'f_volume']]
		for idx, row in df_cp.iterrows():
			try:
#			cur.executemany('INSERT INTO daily_info (code, date, open, high, low, close, volume, marcap, amount) ' \
#						'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', data)
				cur.execute('INSERT INTO trading_trend (code, date, foreigner, institution) ' \
							'VALUES(%s,%s,%s,%s)', (row['stock_code'], row['date'], row['f_volume'], row['i_volume']))
	
				conn.commit()
			except pymysql.IntegrityError:
				pass
			except pymysql.Error as e:
				if conn:
					conn.rollback()
				print ("error %s" % e.args[0])

df = pd.read_sql_query('SELECT * FROM trading_trend limit 5', conn)
print(df.head())
print(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
if conn:
    conn.close()
