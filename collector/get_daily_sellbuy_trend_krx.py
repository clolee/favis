#!/usr/bin/python
import requests
import datetime
import pandas as pd
from pandas import DataFrame
import io, os
# user define package import
import sys
sys.path.append("../../favis")
from msgbot.favisbot import favisbot
import util.krx_util as util
import sqlite3

DB_STOCK_MASTER = '../db/stock_master.db'
DB_STOCK_DAILY_INFO = '../db/stock_daily_sellbuy_trend.db'
    
conn = sqlite3.connect(DB_STOCK_MASTER)
cur = conn.cursor()
df_sm = pd.read_sql_query('SELECT * FROM stock_master', conn)
if conn:
    conn.close()
    
conn = sqlite3.connect(DB_STOCK_DAILY_INFO)
cur = conn.cursor()
   
try:
    with conn:
        cur = conn.cursor()
        cur.execute('CREATE TABLE if not exists stock_daily_sellbuy_trend ( stock_code, date TEXT(8),close,rate,volume,i_volume, f_volume,regdate DATETIME DEFAULT CURRENT_TIMESTAMP)')
        cur.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_daily_sellbuy_trend ON stock_daily_sellbuy_trend (stock_code, date)')

#         cur.executemany("INSERT OR REPLACE INTO  stock_daily_info (stock_code, date, open, high, low, close, volume, marcap, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", datalist)
#         conn.commit()
except sqlite3.Error as e:
    if conn:
        conn.rollback()
    print ("error %s" % e.args[0])

print (datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
for idx, row in df_sm.iterrows():
    stock_code = row['code']
    stock_name = row['name']
    #print (row['code'], util.getIsinCode(row['code']))

    year_start = 2010
    year_end = 2016
    for year in range(year_start, year_end + 1):
        print(stock_code, stock_name, year)
        start = datetime.datetime(year, 1, 1).strftime('%Y%m%d')
        end = datetime.datetime(year, 12, 31).strftime('%Y%m%d')

        isu_cd = util.getIsinCode(row['code'])

        path = "./data/"
        filename = path + stock_code + "_" + start + "-" + end + "-trend.xls"
        if not os.path.exists(filename):
            r = util.get_krx_daily_sellbuy_trend(isu_cd, start, end)
            with open(filename, 'wb') as f:
                #f.write(r.content.decode('utf-8'))
                f.write(r)

        
        #df = pd.read_csv(io.StringIO(r.decode("utf-8")), thousands=',')         
        df = pd.read_excel(filename, thousands=',', usecols=['년/월/일', '종가','대비','거래량(주)','기관_순매수(주)','외국인_순매수(주)'])
        df = df.dropna()
        df.columns = ['date','close','rate','volume','i_volume', 'f_volume']
#        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df['date'] = df['date'].str.replace('/','')
        df_cp = df[['date','close','rate','volume','i_volume', 'f_volume']].copy()
        df_cp['stock_code'] = stock_code
#        print(df_cp)
        
        df_cp = df_cp[['stock_code', 'date','close','rate','volume','i_volume', 'f_volume']]
        
        #df_cp.to_sql('stock_daily_info', conn, index=False, if_exists='replace')
        
        #df = pd.read_sql_query('SELECT * FROM stock_daily_info limit 5', conn)
#        print(df_cp.head())
        try:
            with conn:
                df_cp.to_sql('stock_daily_sellbuy_trend', conn, index=False, if_exists='append')
        except sqlite3.IntegrityError:
            pass
        except sqlite3.Error as e:
            if conn:
                conn.rollback()
            print ("error %s" % e.args[0])

df = pd.read_sql_query('SELECT * FROM stock_daily_sellbuy_trend limit 5', conn)
print(df.head())
print(datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
if conn:
    conn.close()
