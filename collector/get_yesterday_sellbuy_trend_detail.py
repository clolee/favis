#!/App/tools/anaconda3/bin/python
# -*- coding: utf-8 -*-
import traceback
import requests
import datetime
import pandas as pd
from pandas import DataFrame
import io, os
import concurrent.futures
# user define package import

import sys
sys.path.append('./')
import util.krx_util as util
import util.favis_util as fu
from util.favis_logger import FavisLogger
import pymysql
import sqlalchemy as sa
from sqlalchemy import exc

# set config
task_id = 'sellbuy_detail'
logger = FavisLogger(task_id, task_id + '_' + datetime.datetime.today().strftime('%Y%m%d'))
###################################################################################
engine = sa.create_engine('mysql+mysqlconnector://mnilcl:Cloud00!@192.168.10.18:3306/favis', echo=False)

def main_function(date):
	logger.info('\n\n' + str(datetime.datetime.today()) + ' : start...' + date )
	starttime = datetime.datetime.now()

	conn = fu.get_favis_mysql_connection()
	cur = conn.cursor()
	logger.info("1) get krx trading trend")
	#df_sm = pd.read_sql_query("SELECT code FROM stock_info ORDER BY code ASC", conn)
	df_sm = pd.read_sql_query("SELECT code FROM stock_info WHERE CODE NOT IN (SELECT CODE FROM trading_trend WHERE DATE = '"+ date +"') ORDER BY code ASC", conn)
	logger.info(df_sm.values.flatten())
	endtime = datetime.datetime.now()
	if conn:
		conn.close()


	cnt = 0
	for stock_code in df_sm.values.flatten():
		isu_cd = util.getIsinCode(stock_code)
		#print (stock_code, stock_name, isu_cd)

		r = util.get_krx_sellbuy_detail(isu_cd, date, date)
		f = io.BytesIO(r)

		df = pd.read_excel(f, thousands=',', usecols=['투자자명','거래량_순매수'])
		if (cnt%100 == 0):
			logger.info(str(cnt), end=',', flush=True)
#		else:
#			print('.', end='', flush=True)
		df['stock_code'] = stock_code
		df = df.pivot(index='stock_code', columns='투자자명', values='거래량_순매수')
		del df.index.name
		df.columns = ['personal', 'nation_local','financial_investment','institution','etc_fin','etc_ins','etc_for','insurance',\
				'private_equity_fund','pension_fund','foreigner','bank','investment_trust','total']
		df['date'] = date
		df['code'] = stock_code

		df_cp = df[['code','date','personal', 'nation_local','financial_investment','institution','insurance',\
				'private_equity_fund','pension_fund','foreigner','bank','investment_trust']]

		if len(df_cp) == 0 :
			logger.debug (stock_code +' ' + isu_cd+' ' + ' data not found!!')
		else :		
			try:
				df_cp.to_sql(name='trading_trend', con=engine, if_exists = 'append', index=False)
				cnt = cnt + 1
			except exc.IntegrityError:
				cnt = cnt + 1
				pass

	endtime = datetime.datetime.now()
	logger.info('DATE(%s) count : %s, elaspsedtime : %s ' % date, str(cnt),  str(endtime - starttime))
	logger.info('%s : %s end...' % str(datetime.datetime.today()), task_id)



if __name__ == "__main__":
	try:
		starttime = datetime.datetime.now()
		if len(sys.argv) ==  3:
			s_day = sys.argv[1]
			e_day = sys.argv[2]
		else:
			s_day =  (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')   	
			e_day =  (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')   	

		dd = fu.getWorkingDays(s_day, e_day)

		pool = concurrent.futures.ProcessPoolExecutor(max_workers=10)
		pool.map(main_function, dd)
	except Exception as e:
		logger.error ("error %s" % e.args[0])

