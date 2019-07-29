#!/App/tools/anaconda3/bin/python
# -*- coding: utf-8 -*-

import datetime, time
import pandas as pd
import io, os
import concurrent.futures
# user define package import
import sys
sys.path.append('./')

#from msgbot.favisbot import favisbot
import util.krx_util as util
import util.favis_util as fu
import pymysql
import sqlalchemy as sa
from sqlalchemy import exc

import FinanceDataReader as fdr


engine = sa.create_engine('mysql+mysqlconnector://mnilcl:Cloud00!@192.168.10.18:3306/favis', echo=False)

def main_function(code):
    print ("[%s] code : %s ...begin" % (datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'), code))

	# collect data
    day =  (datetime.datetime.today() - datetime.timedelta(1)).strftime('%Y-%m-%d')    
    df = fdr.DataReader(code, day)
    df = df.reset_index()
    df['code'] = code
    print("size :" + str(df.size))
    df.columns = ['date','close', 'open','high','low','volume','change_rate','code']    
    df['date'] = df['date'].astype(str).str.replace('-','')
    df['change_rate'] = round(df['change_rate'], 4)
    print("data size :" + str(df.size))
    try:
        df.to_sql(name='daily_index', con=engine, if_exists = 'append', index=False)
    except exc.IntegrityError as e:
        print ("error %s" % e.args[0])
        pass
    except Exception as e:
        print ("error %s" % e.args[0])
    print ("[%s] code : %s ...end" % (datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'), code))


if __name__ == "__main__":
    conn = fu.get_favis_mysql_connection()
    cur = conn.cursor()
    df_sm = pd.read_sql_query('SELECT code FROM index_info ORDER BY code ASC', conn)
    print(df_sm.values.flatten())
    
    pool = concurrent.futures.ProcessPoolExecutor(max_workers=1)
    pool.map(main_function, df_sm.values.flatten())

    #main_function('CL')
    if conn:
        conn.close()
