#!/usr/bin/python
import requests
import datetime
import pandas as pd
from pandas import DataFrame
# user define package import
import sys
sys.path.append("./")
#from msgbot.favisbot import favisbot
import util.favis_util as favis_util
from util.favis_logger import FavisLogger
import pymysql
import json

# set config
task_id = 'index_price'
logger = FavisLogger(task_id, task_id + '_' + datetime.datetime.today().strftime('%Y%m%d'))
###################################################################################
logger.info('\n\n' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...')
starttime = datetime.datetime.now()

conn = favis_util.get_favis_mysql_connection()
cur = conn.cursor()

index_types = ['KOSPI', 'KOSDAQ', 'KPI200']

for index_type in index_types:
	url = "http://m.stock.naver.com/api/json/sise/dailySiseIndexListJson.nhn?code="+index_type+"&pageSize=1"

	r = requests.get(url).content
	df = json.loads(r.decode('utf-8'))
	rows = df['result']['siseList']
	print(rows)

	cnt = 0
	for row in rows:
		date = str(row['dt'])
		close = row['ncv']
		open = row['ov']
		high = row['hv']
		low = row['lv']
		rate = row['cv']
		try:
			logger.debug (index_type + ',' + date + ',' + str(close))

			cur.execute("INSERT INTO  daily_index (index_type, date, open, high, low, close) VALUES (%s, %s, %s, %s, %s, %s)", 
			(index_type, date, open, high, low, close))
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


