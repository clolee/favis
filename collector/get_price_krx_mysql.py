#!/usr/bin/python
import requests
import datetime, time
import pandas as pd
import io, os
import multiprocessing
# user define package import
favis_path = "/app/favis/"

import sys
sys.path.append(favis_path)
from msgbot.favisbot import favisbot
import util.krx_util as util
import util.favis_util as favis_util
from util.favis_logger import FavisLogger
import pymysql

from sqlalchemy import create_engine

# set config
task_id = 'price'
logger = FavisLogger(task_id, task_id + '_' + datetime.datetime.today().strftime('%Y%m%d'))
###################################################################################

engine = create_engine('mysql+mysqlconnector://root:ckdfh76!!@localhost:3306/favis', echo=False)


def main_function(row):
	s_day = sys.argv[1]

	if len(sys.argv) ==  2 :
		e_day = s_day
	else:
		e_day = sys.argv[2]

	stock_code = row

	logger.info('\n\n' + str(datetime.datetime.today()) + ' : price : ' + stock_code + ' start...' + s_day + '-' +e_day)

	starttime = datetime.datetime.now()

	isu_cd = util.getIsinCode(stock_code)
#		logger.debug (stock_code +' ' + stock_name+' ' +isu_cd)

	path = "/app/favis/collector/data/"
	filename = path + stock_code + "_price.xls"
	r = util.get_krx_daily_info(isu_cd, s_day, e_day)
	with open(filename, 'wb') as f:
		f.write(r)

	df = pd.read_excel(filename, thousands=',', usecols=['년/월/일', '종가','거래량(주)','시가','고가','저가', '시가총액(백만)','상장주식수(주)'])

	df.columns = ['date','close','volume','open','high','low', 'marcap','amount']
	df['date'] = df['date'].str.replace('/','')
	df_cp = df[['date','close','volume','open','high','low', 'marcap','amount']].copy()
	df_cp['stock_code'] = stock_code
	
	df_cp = df_cp[['stock_code', 'date', 'open', 'high', 'low', 'close', 'volume', 'marcap', 'amount']]

	df_cp.to_sql(name='daily_info', con=engine, if_exists = 'append', index=False)

	endtime = datetime.datetime.now()
	logger.info('elaspsedtime : ' + str(endtime - starttime))
	logger.info(str(datetime.datetime.today()) + ' : ' + task_id + ' end...')




# main
if len(sys.argv) ==  3 or len(sys.argv) ==  2:
	conn = favis_util.get_favis_mysql_connection()
	cur = conn.cursor()
	df_sm = pd.read_sql_query('SELECT code FROM stock_info ORDER BY code ASC', conn)
	print(df_sm.values.flatten())
#	dd = pd.Series(pd.bdate_range('20180711', '20180715').format())
	pool = multiprocessing.Pool(processes=5)
#	pool.map(main_function, favis_util.daterange(startdate, enddate))
	pool.map(main_function, df_sm.values.flatten())
#	pool.close
#	pool.join()

	if conn:
		conn.close()
else:
	print("Please put the date")
	exit()
