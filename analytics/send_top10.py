#!/App/tools/anaconda3/bin/python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime
import pandas as pd
from pandas import DataFrame
# user define package import
# user define package import
import sys
sys.path.append('./')
from msgbot.favisbot import favisbot
import util.favis_util as fu
import pymysql
# set config
###################################################################################
try:
    conn = fu.get_favis_mysql_connection()
except pymysql.Error as e:
    print ("error %s" % e.args[0])


cur = conn.cursor()


day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')
#day = '20160801'

query = "select (@order:=@order+1) as s_order, t.* \
			from( \
			select si.name, si.market, si.sector, (a.roa_num + b.per_num) as sum, a.code, a.roa_num, b.per_num, a.roa, b.per \
			from \
			(select (@roa_order:=@roa_order+1) as roa_num, code, roe, roa, per, pbr from financial_info \
			where (@roa_order :=0) = 0 and date = '201603' and roa != 0 order by  roa desc) a, \
			(select (@per_order:=@per_order+1) as per_num, code, per, eps, bps, pbr, dividend_rate  from daily_stock_index \
			where (@per_order :=0) = 0 and date = '" + day + "' and per != 0 order by per asc) b, \
			(select * from stock_info) si \
			where a.code = b.code and a.code = si.code  \
			order by (a.roa_num + b.per_num) asc \
			limit 10) t \
			where (@order :=0) = 0"
cur.execute(query)

df = pd.read_sql(query, conn)
print(df)
if conn:
    conn.close()

ret = ""
for d in df.iterrows():
	ret = ret + str(d[1]['s_order'])+"|"+ str(d[1]['code'])+"|"+ str(d[1]['name'])+"|"+ str(d[1]['sector'])+"|"+ str(d[1]['per'])+"|"+ str(d[1]['roa']) + "\n"
#	print("%2d|%s|%s|%s|%f|%f" % (d[1]['s_order'], str(d[1]['code']), str(d[1]['name']), str(d[1]['sector']), d[1]['per'], d[1]['roa']))


print(ret)
bot = favisbot()
bot.whisper('plain',"day_top10(%s) \n %s" % (day,  ret))

