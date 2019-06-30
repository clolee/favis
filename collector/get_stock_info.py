# -*- coding: utf-8 -*-

import sys
import FinanceDataReader as fdr
import pymysql

sys.path.append("./")
# User Defined Modules
import util.favis_util as fu

if __name__ == "__main__":
    print("1) get krx stock master")
    # KOREA[KRX, KOSPI, KOSDAQ, KONEX], AMERICA[NASDAQ, NYSE, AMEX, SP500]
    MARKET_KOREA = ['KOSPI', 'KOSDAQ','KONEX']
    MARKET_AMERICA = ['NASDAQ','NYSE','AMEX','SP500']


    df = fdr.StockListing('KONEX')
    df = df.fillna('')

    conn = fu.get_favis_mysql_connection()
    cur = conn.cursor()

    # stock_desc 테이블  쓰기
    df_desc = df[['Symbol', 'Name', 'Sector', 'Industry']].copy()
    df_desc['market'] = ''

    print("2) get stock desc and update db")
    cnt = 0
    for idx, row in df_desc.iterrows():
        try:
#            print (row['Symbol'], row['Name'], 'KOSPI', row['Sector'],'|', row['Industry'])
            cur.execute('INSERT INTO stock_info (code, name, market, sector, description, updateddt) ' \
                        'VALUES(%s,%s,%s,%s,%s, current_timestamp)', (row['Symbol'], row['Name'], 'KONEX', row['Sector'], row['Industry']))
            conn.commit()
        
            cnt = cnt + 1
            if (cnt % 100) == 0:
                print("3) get krx stock master (%s)" % cnt)

        except pymysql.IntegrityError:
            cur.execute('UPDATE stock_info SET name=%s, updateddt=current_timestamp WHERE code=%s', (row['Name'], row['Symbol']) )
#				print ("IntegrityError %s" % row['code'])
            conn.commit()
            pass
        except pymysql.Error as e:
            if conn:
                conn.rollback()
            print ("error %s" % e.args[0])

    
    print ("total %s record inserted" % cnt)
    conn.close()        