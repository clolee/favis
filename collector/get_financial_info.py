#!/usr/bin/python
import requests
import datetime, time
import pandas as pd
from pandas import DataFrame
import io, os
# user define package import
import sys
sys.path.append('../../favis')
from msgbot.favisbot import favisbot
import util.krx_util as util
import pymysql

conn = pymysql.connect(host='localhost',
							 user='root',
							 password='ckdfh76!!',
							 db='favis',
							 charset='utf8mb4',
							 cursorclass=pymysql.cursors.DictCursor)

cur = conn.cursor()

# main
df_sm = pd.read_sql_query("SELECT * FROM stock_info order by code asc", conn)

target_dates = ['201503',	'201506','201509','201512','201603']

print (datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
for idx, row in df_sm.iterrows():
	stock_code = row['code']
	stock_name = row['name']
	print (stock_code, stock_name)

	try:
		url = 'http://companyinfo.stock.naver.com/v1/company/ajax/cF1001.aspx?cmp_cd=' + stock_code + '&fin_typ=0&freq_typ=Q'
		df = pd.read_html(url, header=0)
		df = df[0].replace('NaN', '-99')

		df = df[[0,1,2,3,4,5]]
		df.columns = ['name','201503','201506','201509','201512' ,'201603']
		df = df.T
		df_s = df[[0,1,3,6,7,8,11,12,13,14,18,19,20,21,22,23,24,25,26,27, 28, 31]]
		#df_s = df_s.drop(df_s.index[[1,2]])

		idx = 1
		for target_date in target_dates:
	#		print(target_date, stock_code, df_s[0][idx], df_s[1][idx], df_s[3][idx], df_s[6][idx], df_s[7][idx], df_s[8][idx], df_s[11][idx], df_s[12][idx], df_s[13][idx]
	#						, df_s[14][idx], df_s[18][idx], df_s[19][idx], df_s[20][idx], df_s[21][idx], df_s[22][idx], df_s[23][idx], df_s[24][idx], df_s[25][idx], df_s[26][idx]
	#						, df_s[27][idx], df_s[28][idx], df_s[31][idx] )
			try:
				cur.execute('INSERT INTO financial_info (date,code,매출액,영업이익,당기순이익,자산총계,부채총계,자본총계,자본금,영업현금흐름,투자현금흐름,재무현금흐름,영업이익률,순이익률,roe,roa,부채비율,자본유보율,eps,per,bps,pbr,배당금액,주식수) ' \
							'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s,%s)',
							(target_date,stock_code, df_s[0][idx], df_s[1][idx], df_s[3][idx], df_s[6][idx], df_s[7][idx], df_s[8][idx], df_s[11][idx], df_s[12][idx], df_s[13][idx]
							, df_s[14][idx], df_s[18][idx], df_s[19][idx], df_s[20][idx], df_s[21][idx], df_s[22][idx], df_s[23][idx], df_s[24][idx], df_s[25][idx], df_s[26][idx]
							, df_s[27][idx], df_s[28][idx], df_s[31][idx] ))
				conn.commit()
			except pymysql.IntegrityError:
				pass
			except pymysql.Error as e:
				if conn:
					conn.rollback()
				print ("error %s %s" % (target_date, e))
			finally:
				idx = idx + 1
	except KeyError:
		print(stock_code, ' Key Error')
		pass

if conn:
	conn.close()

