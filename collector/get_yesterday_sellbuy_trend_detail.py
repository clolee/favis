#!/usr/bin/python
import requests
import datetime
import pandas as pd
from pandas import DataFrame
import io, os
# user define package import
favis_path = "/app/favis/"
import sys
sys.path.append(favis_path)
from msgbot.favisbot import favisbot
import util.krx_util as util
import util.favis_util as favis_util
from util.favis_logger import FavisLogger
import pymysql

# set config
task_id = 'sellbuy_detail'
logger = FavisLogger(task_id, task_id + '_' + datetime.datetime.today().strftime('%Y%m%d'))
###################################################################################

logger.info('\n\n' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...')
starttime = datetime.datetime.now()

where_cond = ''
if len(sys.argv) > 1:
	start_code = sys.argv[1]
	logger.debug(start_code)
	where_cond = "where code > '" + start_code + "'"

conn = favis_util.get_favis_mysql_connection()
cur = conn.cursor()

query = 'SELECT * FROM stock_info '+ where_cond +' ORDER BY code ASC'
logger.debug(query)
df_sm = pd.read_sql_query(query, conn)

day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')
#day = '20160603'
cnt = 0
for idx, row in df_sm.iterrows():
	stock_code = row['code']
	stock_name = row['name']

	isu_cd = util.getIsinCode(stock_code)
	#print (stock_code, stock_name, isu_cd)

	path = "/exdata/collect_temp/"
	filename = path + stock_code + "_sellbuy_trend_detail.xls"
	r = util.get_krx_sellbuy_detail(isu_cd, day, day)
	with open(filename, 'wb') as f:
		f.write(r)

	df = pd.read_excel(filename, thousands=',', usecols=['투자자명','거래량_순매수'])
	df['stock_code'] = stock_code
	df = df.pivot(index='stock_code', columns='투자자명', values='거래량_순매수')
	del df.index.name
	df.columns = ['personal', 'nation_local','financial_investment','institution','etc_fin','etc_ins','etc_for','insurance',\
              'private_equity_fund','pension_fund','foreigner','bank','investment_trust','total']
	df['date'] = day
	df['stock_code'] = stock_code

	df_cp = df[['stock_code','date','personal', 'nation_local','financial_investment','institution','insurance',\
              'private_equity_fund','pension_fund','foreigner','bank','investment_trust']]
	
	if len(df_cp) == 0 :
		logger.debug (stock_code +' ' + stock_name+' ' +isu_cd+' ' + ' data not found!!')
	else :		
		try:
			row = df_cp.ix[0]
			logger.debug (stock_code +' ' + stock_name+' ' +isu_cd + ' : ' + str(row['institution']))
			cur.execute('INSERT INTO trading_trend (code, date, personal, nation_local,financial_investment,institution,insurance,private_equity_fund,pension_fund,foreigner,bank,investment_trust) ' \
						'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', \
						(row['stock_code'],row['date'],str(row['personal']), str(row['nation_local']),str(row['financial_investment']), 
						 str(row['institution']),str(row['insurance']),str(row['private_equity_fund']),str(row['pension_fund']),str(row['foreigner']),
						 str(row['bank']),str(row['investment_trust'])))

			conn.commit()
		except pymysql.IntegrityError:
			pass
		except pymysql.Error as e:
			if conn:
				conn.rollback()
			logger.error ("error %s" % e.args[0])
		cnt = cnt + 1
if conn:
    conn.close()

endtime = datetime.datetime.now()
logger.info('count :' + str(cnt) + ', elaspsedtime : ' + str(endtime - starttime))
logger.info(str(datetime.datetime.today()) + ' : ' + task_id + ' end...')



