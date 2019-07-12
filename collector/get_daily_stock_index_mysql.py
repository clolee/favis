#!/usr/bin/python
import requests
import datetime, time
import pandas as pd
from pandas import DataFrame
import io, os
import concurrent.futures
# user define package import
import sys
sys.path.append('./')

#from msgbot.favisbot import favisbot
import util.krx_util as util
import util.favis_util as fu
import pymysql
import sqlalchemy as sa
from sqlalchemy import exc

engine = sa.create_engine('mysql+mysqlconnector://mnilcl:Cloud00!@192.168.10.18:3306/favis', echo=False)

def main_function(day):
	print ("collect date : " + day)
	# main
	conn = fu.get_favis_mysql_connection()
	cur = conn.cursor()

	df_sm = pd.read_sql_query("SELECT code FROM stock_info WHERE CODE NOT IN (SELECT CODE FROM daily_stock_index WHERE DATE = '"+ day +"') ORDER BY code ASC", conn)
		
	cnt = 0
	print (datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
	for idx, row in df_sm.iterrows():
		stock_code = row['code']
		#print (row['code'], util.getIsinCode(row['code']))

		if (cnt%100 == 0):
			print(str(cnt), end=',', flush=True)

		isu_cd = util.getIsinCode(stock_code)

		r = util.get_krx_daily_stock_index(isu_cd, 'A'+stock_code, day, day)
		f = io.BytesIO(r)

		df = pd.read_excel(f, thousands=',', usecols=['일자', '종목코드','EPS','PER','BPS','PBR','주당배당금','배당수익률'], converters={'종목코드':str})
		df.columns = ['date','stock_code','eps','per','bps','pbr', 'dividend','dividend_rate']
		df['date'] = df['date'].str.replace('/','')
		
		print(str(cnt) + " " + stock_code + " : " + str(len(df)))
		#data = [tuple(x) for x in df_cp.to_records(index=False)]
		#df.set_index('Date', inplace=True)
		#df = df.sort_index(0, ascending=True)
		if len(df) == 0 :
			print(stock_code +' ' + isu_cd+' ' + ' data not found!!')		
		else :		
			try:
				df.to_sql(name='daily_stock_index', con=engine, if_exists = 'append', index=False)
				cnt = cnt + 1
			except exc.IntegrityError:
				cnt = cnt + 1
				pass

	#df = pd.read_sql_query('SELECT * FROM daily_info limit 5', conn)
	#print(df.head())
	#print(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
	if conn:
		conn.close()
	print(day + " : " + str(cnt))


if __name__ == "__main__":
	try:
		starttime = datetime.datetime.now()
		if len(sys.argv) ==  3:
			s_day = sys.argv[1]
			e_day = sys.argv[2]
		else:
			s_day =  (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')   	
			e_day =  (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')   	

		dd = fu. getDateRangeList(s_day, e_day)
		print(dd)

		pool = concurrent.futures.ProcessPoolExecutor(max_workers=10)
		pool.map(main_function, dd)
	except Exception as e:
		print ("error %s" % e.args[0])
