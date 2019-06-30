# -*- coding: utf-8 -*-
import datetime, time
import io, os, sys
import multiprocessing
import pandas as pd
import FinanceDataReader as fdr
import pymysql
import sqlalchemy as sa
from sqlalchemy import exc

sys.path.append("./")
# User Defined Modules
import util.favis_util as fu

# set config
task_id = 'price'
###################################################################################
engine = sa.create_engine('mysql+mysqlconnector://mnilcl:Cloud00!@192.168.10.18:3306/favis', echo=False)

def main_function(row):
    if len(sys.argv) ==  2 :
        s_day = sys.argv[1]
    else:
        s_day = '1900-01-01'

    stock_code = row

    starttime = datetime.datetime.now()

    df = fdr.DataReader(stock_code, s_day)
    df = df.reset_index()
    df.columns = ['date','open','high','low','close','volume','change']
#    df['date'] = df['date'].str.replace('-','')
    df_cp = df[['date','open','high','low','close','volume','change']].copy()
    df_cp['code'] = stock_code
    del df_cp['change']
	
#    df_cp = df_cp[['stock_code', 'date', 'open', 'high', 'low', 'close', 'volume', 'marcap', 'amount']]

    print(df_cp.head())
    try:
        df_cp.to_sql(name='daily_info', con=engine, if_exists = 'append', index=False)
    except exc.IntegrityError:
        pass

    endtime = datetime.datetime.now()
    print('elaspsedtime : ' + str(endtime - starttime))
    print(str(datetime.datetime.today()) + ' : ' + task_id + ' end...')

#	logger.info('elaspsedtime : ' + str(endtime - starttime))
#	logger.info(str(datetime.datetime.today()) + ' : ' + task_id + ' end...')




# main
if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) ==  1 or len(sys.argv) ==  2:
        conn = fu.get_favis_mysql_connection()
        cur = conn.cursor()

        df_sm = pd.read_sql_query('SELECT code FROM stock_info ORDER BY code ASC', conn)
        print(df_sm.values.flatten())

        pool = multiprocessing.Pool(processes=10)
        pool.map(main_function, df_sm.values.flatten())

        if conn:
            conn.close()
    else:
        print("Please put the date")
        exit()
