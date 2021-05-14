#!/App/tools/anaconda3/bin/python
# -*- coding: utf-8 -*-
import requests
import datetime, time
import pandas as pd
import io, os
import concurrent.futures
# user define package import

import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

# User Defined Modules
import util.favis_util as fu
#from msgbot.favisbot import favisbot
import util.krx_util as util
#from util.favis_logger import FavisLogger
import pymysql
import sqlalchemy as sa
from sqlalchemy import exc

# set config
task_id = 'price'
#logger = FavisLogger(task_id, task_id + '_' + datetime.datetime.today().strftime('%Y%m%d'))
###################################################################################
engine = sa.create_engine('mysql+mysqlconnector://mnilcl:Cloud00!@192.168.0.6:3306/favis', echo=False)

def main_function(stock_code, isu_cd):
    time.sleep(1.0)
    print(stock_code + ":" + isu_cd)
    if len(sys.argv) ==  3:
        s_day = sys.argv[1] 
        e_day = sys.argv[2]        
    else:
        s_day =  (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')       
        e_day =  (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')       
#    s_day = '19000101'
#    e_day = '20190701'
#    print('\n [' + stock_code + ']' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...' + s_day + '-' +e_day)
#    logger.info('\n\n' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...' + s_day + '-' +e_day)
    print('\n\n' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...' + s_day + '-' +e_day)
#

    starttime = datetime.datetime.now()

#    day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')

    s_day = s_day.replace('-','')
    e_day = e_day.replace('-','')

    f = util.get_krx_price(isu_cd, s_day, e_day)

    usecols = ['일자','종가','대비','등락률','시가','고가','저가', '거래량','거래대금','시가총액','상장주식수']

    df = pd.read_excel(f, thousands=',', usecols=usecols)

    df.columns = ['date','close','comparison','change_rate','open','high','low','volume', 'trading_value', 'marcap','amount']
    df['date'] = df['date'].str.replace('/','')
    df_cp = df[['date','close','change_rate','open','high','low','volume', 'trading_value', 'marcap','amount']].copy()
    df_cp['code'] = stock_code
    
    df_cp = df_cp[['code', 'date', 'open', 'high', 'low', 'close', 'change_rate','volume', 'trading_value', 'marcap', 'amount']]
    #logger.debug(df_cp.head())
    #data = [tuple(x) for x in df_cp.to_records(index=False)]
    #logger.debug(df_cp[['stock_code', 'Date','Open','High','Low','Close','Volume','marcap','amount']].head())

    try:
        df_cp.to_sql(name='price', con=engine, if_exists = 'append', index=False)
    except exc.IntegrityError:
        pass

    # for idx, row in df_cp.iterrows():
    #     try:
    #             cur.execute('INSERT INTO daily_info (code, date, open, high, low, close, volume, marcap, amount) ' \
    #                         'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', (row['stock_code'], row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['marcap'], row['amount']))
        
    #             conn.commit()
    #     except pymysql.IntegrityError:
    #         pass
    #     except pymysql.Error as e:
    #         if conn:
    #             conn.rollback()
    #         logger.debug ("error %s" % e.args[0])
    # cnt = cnt +1 
    
    endtime = datetime.datetime.now()
    print('[' + stock_code + ']' 'count :' + str(len(df_cp)) + ', elaspsedtime : ' + str(endtime - starttime))
#    print(str(datetime.datetime.today()) + ' : ' + task_id + ' end...')

# main
#if len(sys.argv) ==  30:
#    startdate = sys.argv[1]
    # enddate = sys.argv[2]
    # logger.debug('term : ' + startdate + '-' + enddate)
    # dd = pd.Series(pd.bdate_range(startdate, enddate).format())
    # print(dd)
    # pool = multiprocessing.Pool(processes=5)
#    pool.map(main_function, favis_util.daterange(startdate, enddate))
#    pool.map(main_function, dd)
#    pool.close
#    pool.join()

#    for d in favis_util.daterange(startdate, enddate):
#        logger.debug('day : ' + d.strftime('%Y%m%d'))
#        main_function(d.strftime('%Y%m%d'))
#else:
#    day = (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y%m%d')
#    print("day:" + day)
#    main_function(day)
#    main_function(sys.argv[1], sys.argv[2])

if __name__ == "__main__":
    
    conn = fu.get_favis_mysql_connection()
    cur = conn.cursor()

    starttime = datetime.datetime.now()
    print("1) get krx stock price")
    df_sm = pd.read_sql_query('SELECT code, std_code FROM stock_info ORDER BY code ASC', conn)
#    df_sm = pd.read_sql_query('SELECT code FROM stock_info WHERE CODE NOT IN (SELECT distinct(CODE) FROM daily_info) ORDER BY code ASC', conn)
#    print(df_sm.values.flatten())
    print(df_sm.columns)

#    main_function('000020','KR7000020008')
    pool = concurrent.futures.ProcessPoolExecutor(max_workers=1)
    pool.map(main_function, df_sm.code.values.flatten(), df_sm.std_code.values.flatten())

    endtime = datetime.datetime.now()
    if conn:
        conn.close()        
