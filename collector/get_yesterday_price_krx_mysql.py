# -*- coding: utf-8 -*-

import requests
import datetime, time
import pandas as pd
import io, os
import multiprocessing
# user define package import

import sys
sys.path.append('/App/favis')
#from msgbot.favisbot import favisbot
import util.krx_util as util
import util.favis_util as favis_util
from util.favis_logger import FavisLogger
import pymysql

# set config
task_id = 'price'
logger = FavisLogger(task_id, task_id + '_' + datetime.datetime.today().strftime('%Y%m%d'))
###################################################################################


def main_function(s_day, e_day):
	print('\n\n' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...' + s_day + '-' +e_day)
	logger.info('\n\n' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...' + s_day + '-' +e_day)
	conn = favis_util.get_favis_mysql_connection()
	cur = conn.cursor()
	df_sm = pd.read_sql_query('SELECT * FROM stock_info ORDER BY code ASC', conn)

	starttime = datetime.datetime.now()

#	day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')

	s_day = s_day.replace('-','')
	e_day = e_day.replace('-','')

	cnt =0
	for idx, row in df_sm.iterrows():
		stock_code = row['code']
		stock_name = row['name']

		isu_cd = util.getIsinCode(stock_code)
#		logger.debug (stock_code +' ' + stock_name+' ' +isu_cd)

#		path = "/app/favis/collector/data/"
#		filename = path + stock_code + "_price.xls"
		r = util.get_krx_daily_info(isu_cd, s_day, e_day)
		filename = io.BytesIO(r)
#		with open(filename, 'wb') as f:
#			f.write(r)

		df = pd.read_excel(filename, thousands=',', usecols=['년/월/일', '종가','거래량(주)','시가','고가','저가', '시가총액(백만)','상장주식수(주)'])

		df.columns = ['date','close','volume','open','high','low', 'marcap','amount']
		df['date'] = df['date'].str.replace('/','')
		df_cp = df[['date','close','volume','open','high','low', 'marcap','amount']].copy()
		df_cp['stock_code'] = stock_code
		
		df_cp = df_cp[['stock_code', 'date', 'open', 'high', 'low', 'close', 'volume', 'marcap', 'amount']]
		#logger.debug(df_cp.head())
		#data = [tuple(x) for x in df_cp.to_records(index=False)]
		#logger.debug(df_cp[['stock_code', 'Date','Open','High','Low','Close','Volume','marcap','amount']].head())

		for idx, row in df_cp.iterrows():
			try:
					cur.execute('INSERT INTO daily_info (code, date, open, high, low, close, volume, marcap, amount) ' \
								'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', (row['stock_code'], row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['marcap'], row['amount']))
			
					conn.commit()
			except pymysql.IntegrityError:
				pass
			except pymysql.Error as e:
				if conn:
					conn.rollback()
				logger.debug ("error %s" % e.args[0])
		cnt = cnt +1 

	endtime = datetime.datetime.now()
	logger.info('count :' + str(cnt) + ', elaspsedtime : ' + str(endtime - starttime))
	logger.info(str(datetime.datetime.today()) + ' : ' + task_id + ' end...')

	if conn:
		conn.close()



# main
if len(sys.argv) ==  30:
	startdate = sys.argv[1]
	enddate = sys.argv[2]
	logger.debug('term : ' + startdate + '-' + enddate)
	dd = pd.Series(pd.bdate_range(startdate, enddate).format())
	print(dd)
	pool = multiprocessing.Pool(processes=5)
#	pool.map(main_function, favis_util.daterange(startdate, enddate))
	pool.map(main_function, dd)
#	pool.close
#	pool.join()

#	for d in favis_util.daterange(startdate, enddate):
#		logger.debug('day : ' + d.strftime('%Y%m%d'))
#		main_function(d.strftime('%Y%m%d'))
else:
#	day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')
#	print("day:" + day)
#	main_function(day)
	main_function(sys.argv[1], sys.argv[2])

