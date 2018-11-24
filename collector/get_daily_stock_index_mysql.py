#!/usr/bin/python
import requests
import datetime, time
import pandas as pd
from pandas import DataFrame
import io, os
# user define package import
import sys
sys.path.append('/app/favis')
from msgbot.favisbot import favisbot
import util.krx_util as util
import util.favis_util as favisutil
import pymysql


def main_function(day):
	print ("collect date : " + day)
	# main
	conn = pymysql.connect(host='localhost',
								 user='root',
								 password='ckdfh76!!',
								 db='favis',
								 charset='utf8mb4',
								 cursorclass=pymysql.cursors.DictCursor)

	cur = conn.cursor()

	df_sm = pd.read_sql_query("SELECT * FROM stock_info order by code asc", conn)
		
	count = 0
	print (datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
	for idx, row in df_sm.iterrows():
		stock_code = row['code']
		stock_name = row['name']
		#print (row['code'], util.getIsinCode(row['code']))

		isu_cd = util.getIsinCode(stock_code)

		path = "/exdata/collect_temp/"
		filename = path + 'stockindex_' + stock_code + "_" + day + ".xls"
		if not os.path.exists(filename):
			r = util.get_krx_daily_stock_index(isu_cd, 'A'+stock_code, day, day)
			with open(filename, 'wb') as f:
				#f.write(r.content.decode('utf-8'))
				f.write(r)

		df = pd.read_excel(filename, thousands=',', usecols=['일자', '종목코드','EPS','PER','BPS','PBR','주당배당금','배당수익률'], converters={'종목코드':str})
		df.columns = ['date','stock_code','eps','per','bps','pbr', 'dividend','dividend_rate']
		df['date'] = df['date'].str.replace('/','')
		
		print(str(count) + " " + stock_code + " : " + str(len(df)))
		#data = [tuple(x) for x in df_cp.to_records(index=False)]
		#df.set_index('Date', inplace=True)
		#df = df.sort_index(0, ascending=True)
		count= count+1
		for idx, row in df.iterrows():
			try:
	#			cur.executemany('INSERT INTO daily_info (code, date, open, high, low, close, volume, marcap, amount) ' \
	#						'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', data)
				cur.execute('INSERT INTO daily_stock_index (code, date, eps, per, bps, pbr, dividend, dividend_rate) ' \
							'VALUES(%s,%s,%s,%s,%s,%s,%s,%s)', (row['stock_code'], row['date'], row['eps'], row['per'], row['bps'], row['pbr'], row['dividend'], row['dividend_rate']))

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
	print(day + " : " + str(count))


# main
if len(sys.argv) ==  3:
	startdate = sys.argv[1]
	enddate = sys.argv[2]
	print('term : ' + startdate + '-' + enddate)
	for d in favisutil.daterange(startdate,enddate):
		main_function(d.strftime('%Y%m%d'))
else:
	day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')
	print("day:" + day)
	main_function(day)

