# -*- coding: utf-8 -*-
import requests
import sys, os, io
import datetime, time
from bs4 import BeautifulSoup
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
# User Defined Modules
import util.favis_util as fu
import util.krx_util as ku

import sqlalchemy as sa
from sqlalchemy import exc
#####################################################################################
DB_USER='mnilcl' 
DB_PASS='Cloud00!'
DB_HOST='192.168.0.6'
DB_PORT='3306'
DATABASE='favis'
connect_string = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DATABASE)
engine = sa.create_engine(connect_string, convert_unicode=True, echo=False)

# sector, wics, name_en
def get_sector(code):
    name_en, sector, wics, market = 'nan', 'nan', 'nan', 'nan'
    url = 'http://companyinfo.stock.naver.com/v1/company/c1010001.aspx?cmp_cd=' + code
    r = requests.get(url)
    soup = BeautifulSoup(r.text,"lxml")
    td = soup.find('td', {'class':'cmp-table-cell td0101'})
    if td == None:
        return sector, wics, name_en
        
    dts = td.findAll('dt')

    # dts[1], name_en
    name_en = dts[1].text
    
    # dts[2], sector
    s = dts[2]
    if s.text.find('KOSPI :') >= 0:
        market = 'KOSPI'
        sector = s.text.split(' : ')[1]
    elif dts[2].text.find('KOSDAQ :') >= 0:
        market = 'KOSDAQ'
        sector = s.text.split(' : ')[1]
    elif dts[2].text.find('KONEX :') >= 0:
        market = 'KONEX'
        sector = s.text.split(' : ')[1]
            
    # dts[3], wics
    s = dts[3]
    if s.text.find('WICS :') >= 0:
        wics = s.text.split(' : ')[1]

    return market, wics, name_en


# desc, desc_date
def get_desc(code):
    url = 'http://companyinfo.stock.naver.com/v1/company/cmpcomment.aspx'
 
    cmt_text = ' '
    cmt_date = time.strftime("%Y-%m-%d")
 
    if code[-1] == '5': # pre_order
        code = code[0:-1] + '0'
    if code[-1] == '7': # pre_order
        code = code[0:-1] + '0'
 
    headers = { 'Referer': 'http://marketdata.krx.co.kr/mdi', 'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}
    r = requests.post(url, data={'cmp_cd': code}, headers=headers)
    if r.text == "":
        return (cmt_text, cmt_date)
 
    j = json.loads(r.text)
    cmt_date = j['dt'].replace('.', '-')
    cmts = j['data'][0]
    cmts = [cmts['COMMENT_1'], cmts['COMMENT_2'], cmts['COMMENT_3'],  cmts['COMMENT_4'], cmts['COMMENT_5'] ]
    cmt_text = '. '.join(cmts)
    return (cmt_text, cmt_date)    
    
    
if __name__ == "__main__":

    conn = fu.get_favis_mysql_connection()
    cur = conn.cursor()

    starttime = datetime.datetime.now()
    print("1) get krx stock master")
 
    f = ku.get_krx_stock_info()   
    
    usecols = ['단축코드', '표준코드','한글 종목약명', '영문 종목명', '상장일', '시장구분', '상장주식수']
    df = pd.read_excel(f, converters={'단축코드': str}, usecols=usecols)
#    df = pd.read_excel(f)
    df.columns = ['std_code', 'code','name', 'name_en', 'ipo_date', 'market', 'total_stock_count']
    df = df[['code','std_code','name', 'name_en', 'ipo_date', 'market', 'total_stock_count']]
    # stock_desc 테이블  쓰기
    df['sector'] = ''
    df['sector_code'] = ''
    df['updateddt'] = datetime.datetime.now()
    df = df.fillna('')
    print(df)    

    
    print("2) get stock desc and update db")

    try:
        df.to_sql(name='stock_info', con=engine, if_exists = 'append', index=False)
    except exc.IntegrityError:
        pass
    endtime = datetime.datetime.now()

    print('elapsed time : ',  str(endtime - starttime))


    if conn:
        conn.close()