# -*- coding: utf-8 -*-

import requests
import datetime, time
import pandas as pd
import io, os
import concurrent.futures
# user define package import

import sys
sys.path.append("/App/favis")
# User Defined Modules
import util.favis_util as fu
#from msgbot.favisbot import favisbot
import util.krx_util as util
#from util.favis_logger import FavisLogger
import pymysql
import sqlalchemy as sa
from sqlalchemy import exc

# set config
task_id = 'price'
#logger = FavisLogger(task_id, task_id + '_' + datetime.datetime.today().strftime('%Y%m%d'))
###################################################################################
engine = sa.create_engine('mysql+mysqlconnector://mnilcl:Cloud00!@192.168.10.18:3306/favis', echo=False)

def main_function(stock_code):
	if len(sys.argv) ==  3:
		s_day = sys.argv[1]
		e_day = sys.argv[2]    	
	else:
		s_day =  (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')   	
		e_day =  (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')   	
#	s_day = '19000101'
#	e_day = '20190701'
#	print('\n [' + stock_code + ']' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...' + s_day + '-' +e_day)
#	logger.info('\n\n' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...' + s_day + '-' +e_day)
	print('\n\n' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...' + s_day + '-' +e_day)
#

	starttime = datetime.datetime.now()

#	day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')

	s_day = s_day.replace('-','')
	e_day = e_day.replace('-','')

	isu_cd = util.getIsinCode(stock_code)
	r = util.get_krx_daily_info(isu_cd, s_day, e_day)
	f = io.BytesIO(r)

	df = pd.read_excel(f, thousands=',', usecols=['년/월/일', '종가','대비','거래량(주)','시가','고가','저가', '시가총액(백만)','상장주식수(주)'])

	df.columns = ['date','close','change','volume','open','high','low', 'marcap','amount']
	df['date'] = df['date'].str.replace('/','')
	df_cp = df[['date','close','change','volume','open','high','low', 'marcap','amount']].copy()
	df_cp['code'] = stock_code
	
	df_cp = df_cp[['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'marcap', 'amount','change']]
	#logger.debug(df_cp.head())
	#data = [tuple(x) for x in df_cp.to_records(index=False)]
	#logger.debug(df_cp[['stock_code', 'Date','Open','High','Low','Close','Volume','marcap','amount']].head())

	try:
		df_cp.to_sql(name='daily_info', con=engine, if_exists = 'append', index=False)
	except exc.IntegrityError:
		pass

	# for idx, row in df_cp.iterrows():
	# 	try:
	# 			cur.execute('INSERT INTO daily_info (code, date, open, high, low, close, volume, marcap, amount) ' \
	# 						'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', (row['stock_code'], row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['marcap'], row['amount']))
		
	# 			conn.commit()
	# 	except pymysql.IntegrityError:
	# 		pass
	# 	except pymysql.Error as e:
	# 		if conn:
	# 			conn.rollback()
	# 		logger.debug ("error %s" % e.args[0])
	# cnt = cnt +1 
	
	endtime = datetime.datetime.now()
	print('[' + stock_code + ']' 'count :' + str(len(df_cp)) + ', elaspsedtime : ' + str(endtime - starttime))
#	print(str(datetime.datetime.today()) + ' : ' + task_id + ' end...')

# main
#if len(sys.argv) ==  30:
#	startdate = sys.argv[1]
	# enddate = sys.argv[2]
	# logger.debug('term : ' + startdate + '-' + enddate)
	# dd = pd.Series(pd.bdate_range(startdate, enddate).format())
	# print(dd)
	# pool = multiprocessing.Pool(processes=5)
#	pool.map(main_function, favis_util.daterange(startdate, enddate))
#	pool.map(main_function, dd)
#	pool.close
#	pool.join()

#	for d in favis_util.daterange(startdate, enddate):
#		logger.debug('day : ' + d.strftime('%Y%m%d'))
#		main_function(d.strftime('%Y%m%d'))
#else:
#	day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')
#	print("day:" + day)
#	main_function(day)
#	main_function(sys.argv[1], sys.argv[2])

if __name__ == "__main__":
    
	conn = fu.get_favis_mysql_connection()
	cur = conn.cursor()

	starttime = datetime.datetime.now()
	print("1) get krx stock master")
	df_sm = pd.read_sql_query('SELECT code FROM stock_info ORDER BY code ASC', conn)
#	df_sm = pd.read_sql_query('SELECT code FROM stock_info WHERE CODE NOT IN (SELECT distinct(CODE) FROM daily_info) ORDER BY code ASC', conn)
	print(df_sm.values.flatten())
	
	pool = concurrent.futures.ProcessPoolExecutor(max_workers=10)
	pool.map(main_function, df_sm.values.flatten())

	endtime = datetime.datetime.now()
	if conn:
		conn.close()		
