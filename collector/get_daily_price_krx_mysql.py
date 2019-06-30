#!/usr/bin/python
import requests
import datetime, time
import pandas as pd
from pandas import DataFrame
import io, os
# user define package import
import sys
sys.path.append('../../favis')
import util.krx_util as util
import pymysql

conn = pymysql.connect(host='192.168.10.18',
							 user='mnilcl',
							 password='Cloud00!',
							 db='favis',
							 charset='utf8mb4',
							 cursorclass=pymysql.cursors.DictCursor)

cur = conn.cursor()
    
# main
df_sm = pd.read_sql_query('SELECT * FROM stock_info', conn)
    
print (datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
for idx, row in df_sm.iterrows():
	stock_code = row['code']
	stock_name = row['name']
	#print (row['code'], util.getIsinCode(row['code']))

	year_start = 2016
	year_end = 2016
	for year in range(year_start, year_end + 1):
		print(stock_code, stock_name, year)
		start = datetime.datetime(year, 7, 14).strftime('%Y%m%d')
		end = datetime.datetime(year, 12, 31).strftime('%Y%m%d')
	
		isu_cd = util.getIsinCode(stock_code)

		path = "./data/"
		filename = path + stock_code + "_" + start + "-" + end + ".xls"
		if not os.path.exists(filename):
			r = util.get_krx_daily_info(isu_cd, start, end)
			with open(filename, 'wb') as f:
				#f.write(r.content.decode('utf-8'))
				f.write(r)

		df = pd.read_excel(filename, thousands=',', usecols=['년/월/일', '종가','거래량(주)','시가','고가','저가', '시가총액(백만)','상장주식수(주)'])
		df.columns = ['date','close','volume','open','high','low', 'marcap','amount']
		df['date'] = df['date'].str.replace('/','')
		df_cp = df[['date','close','volume','open','high','low', 'marcap','amount']].copy()
		df_cp['stock_code'] = stock_code
		

		df_cp = df_cp[['stock_code', 'date', 'open', 'high', 'low', 'close', 'volume', 'marcap', 'amount']]
		#data = [tuple(x) for x in df_cp.to_records(index=False)]
		#df.set_index('Date', inplace=True)
		#df = df.sort_index(0, ascending=True)
		for idx, row in df_cp.iterrows():
			try:
#			cur.executemany('INSERT INTO daily_info (code, date, open, high, low, close, volume, marcap, amount) ' \
#						'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', data)
				cur.execute('INSERT INTO daily_info (code, date, open, high, low, close, volume, marcap, amount) ' \
							'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', (row['stock_code'], row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['marcap'], row['amount']))
	
				conn.commit()
			except pymysql.IntegrityError:
				pass
			except pymysql.Error as e:
				if conn:
					conn.rollback()
				print ("error %s" % e.args[0])

#df = pd.read_sql_query('SELECT * FROM daily_info limit 5', conn)
#print(df.head())
#print(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
if conn:
	conn.close()

