# -*- coding: utf-8 -*-

import requests
import datetime
import pandas as pd
import io, os
# user define package import

import sys
sys.path.append("./")
#from msgbot.favisbot import favisbot
import util.krx_util as util
import util.favis_util as fu
from util.favis_logger import FavisLogger
import pymysql

# set config
task_id = 'sellbuy'
logger = FavisLogger(task_id, task_id + '_' + datetime.datetime.today().strftime('%Y%m%d'))
###################################################################################

def main_function(day):
	logger.info('\n\n' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...')

	starttime = datetime.datetime.now()
	where_cond = ''

#	if len(sys.argv) > 1:
#		start_code = sys.argv[1]
#		logger.debug(start_code)
#		where_cond = "where code > '" + start_code + "'"

	conn = fu.get_favis_mysql_connection()
	cur = conn.cursor()

	query = 'SELECT * FROM stock_info '+ where_cond +' ORDER BY code ASC'
	logger.debug(query)
	df_sm = pd.read_sql_query(query, conn)

	#day = datetime.datetime.today().strftime('%Y%m%d')
	#day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')
	#day = '20170328'
	logger.debug(day)
	cnt = 0
	for idx, row in df_sm.iterrows():
		stock_code = row['code']
		stock_name = row['name']

		isu_cd = util.getIsinCode(stock_code)
		#logger.debug (stock_code, stock_name, isu_cd)

		r = util.get_krx_daily_sellbuy_trend(isu_cd, day, day)
		f = io.BytesIO(r)

		df = pd.read_excel(f, thousands=',', usecols=['년/월/일', '기관_순매수(주)','외국인_순매수(주)'])
		df = df.dropna()
		df.columns = ['date','i_volume', 'f_volume']
		df['date'] = df['date'].str.replace('/','')
		df_cp = df[['date','i_volume', 'f_volume']].copy()
		df_cp['stock_code'] = stock_code
		df_cp = df_cp[['stock_code', 'date','i_volume', 'f_volume']]
		
		print(df.head())

		if len(df_cp) == 0 :
			logger.debug(stock_code +' ' + stock_name+' ' +isu_cd + ' data not found!!')
		else :		
			logger.debug(stock_code +' ' + stock_name+' ' +isu_cd)
			try:
				row = df_cp.ix[0]
				cur.execute('INSERT INTO trading_trend (code, date, foreigner, institution) ' \
							'VALUES(%s,%s,%s,%s)', (row['stock_code'], row['date'], int(row['f_volume']), int(row['i_volume'])))

				conn.commit()
			except pymysql.IntegrityError:
				pass
			except pymysql.Error as e:
				if conn:
					conn.rollback()
				logger.debug ("error %s" % e.args[0])
			cnt = cnt + 1

	df = pd.read_sql_query('SELECT * FROM trading_trend limit 5', conn)

	endtime = datetime.datetime.now()
#	logger.info('count :', cnt, ', elaspsedtime : ' , (endtime - starttime))
#	logger.info(str(datetime.datetime.today()) + ' : ' + task_id + ' end...')
	if conn:
		conn.close()


# main
if len(sys.argv) ==  3:
	startdate = sys.argv[1]
	enddate = sys.argv[2]
	logger.debug('term : ' + startdate + '-' + enddate)


	for d in fu.getWorkingDays(startdate, enddate):
		main_function(d)
else:
	day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')
	print("day:" + day)
	main_function(day)

