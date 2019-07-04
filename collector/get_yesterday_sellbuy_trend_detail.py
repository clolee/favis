#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import datetime
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
from util.favis_logger import FavisLogger
import pymysql
import sqlalchemy as sa
from sqlalchemy import exc

# set config
task_id = 'sellbuy_detail'
logger = FavisLogger(task_id, task_id + '_' + datetime.datetime.today().strftime('%Y%m%d'))
###################################################################################
engine = sa.create_engine('mysql+mysqlconnector://mnilcl:Cloud00!@192.168.10.18:3306/favis', echo=False)

def main_function(stock_code):
	if len(sys.argv) ==  3:
		s_day = sys.argv[1]
		e_day = sys.argv[2]    	
	else:
		s_day =  (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')   	
		e_day =  (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')   	

	logger.info('\n\n' + str(datetime.datetime.today()) + ' : ' + stock_code + ' start...' + s_day + '-' + e_day)
	starttime = datetime.datetime.now()

	isu_cd = util.getIsinCode(stock_code)
	#print (stock_code, stock_name, isu_cd)

	r = util.get_krx_sellbuy_detail(isu_cd, s_day, e_day)
	f = io.BytesIO(r)

	df = pd.read_excel(f, thousands=',', usecols=['투자자명','거래량_순매수'])
	print(df.head())
	df['stock_code'] = stock_code
	df = df.pivot(index='stock_code', columns='투자자명', values='거래량_순매수')
	del df.index.name
	df.columns = ['personal', 'nation_local','financial_investment','institution','etc_fin','etc_ins','etc_for','insurance',\
			'private_equity_fund','pension_fund','foreigner','bank','investment_trust','total']
	df['date'] = s_day
	df['code'] = stock_code

	df_cp = df[['code','date','personal', 'nation_local','financial_investment','institution','insurance',\
			'private_equity_fund','pension_fund','foreigner','bank','investment_trust']]
	
	if len(df_cp) == 0 :
		logger.debug (stock_code +' ' + isu_cd+' ' + ' data not found!!')
	else :		
		try:
			df_cp.to_sql(name='trading_trend', con=engine, if_exists = 'append', index=False)
		except exc.IntegrityError:
			pass

		# try:
		# 	row = df_cp.iloc[0]
		# 	logger.debug (stock_code +' ' + stock_name+' ' +isu_cd + ' : ' + str(row['institution']))
		# 	cur.execute('INSERT INTO trading_trend (code, date, personal, nation_local,financial_investment,institution,insurance,private_equity_fund,pension_fund,foreigner,bank,investment_trust) ' \
		# 				'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', \
		# 				(row['code'],row['date'],str(row['personal']), str(row['nation_local']),str(row['financial_investment']), 
		# 				str(row['institution']),str(row['insurance']),str(row['private_equity_fund']),str(row['pension_fund']),str(row['foreigner']),
		# 				str(row['bank']),str(row['investment_trust'])))

		# 	conn.commit()
		# except pymysql.IntegrityError:
		# 	pass
		# except pymysql.Error as e:
		# 	if conn:
		# 		conn.rollback()
		# 	logger.error ("error %s" % e.args[0])
# 		cnt = cnt + 1
# if conn:
# 	conn.close()

	endtime = datetime.datetime.now()
	logger.info('count :' + str(cnt) + ', elaspsedtime : ' + str(endtime - starttime))
	logger.info(str(datetime.datetime.today()) + ' : ' + task_id + ' end...')



if __name__ == "__main__":
	conn = fu.get_favis_mysql_connection()
	cur = conn.cursor()

	starttime = datetime.datetime.now()
	print("1) get krx trading trend")
	df_sm = pd.read_sql_query('SELECT code FROM stock_info ORDER BY code ASC', conn)
	print(df_sm.values.flatten())
	
	pool = concurrent.futures.ProcessPoolExecutor(max_workers=10)
	pool.map(main_function, df_sm.values.flatten())

	endtime = datetime.datetime.now()
	if conn:
		conn.close()	    	
