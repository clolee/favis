import pandas as pd
import io
import requests
from datetime import date, datetime, timedelta
from sqlalchemy import create_engine, exc
import pymysql
# MySQL Connector using pymysql
pymysql.install_as_MySQLdb()
import MySQLdb

engine = create_engine('mysql+mysqldb://mnilcl:Cloud00!@192.168.0.6:3306/favis', echo=False)

etfs = [
    {"name":"ARKK", "url":"https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv"},
    {"name":"ARKQ", "url":"https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv"},
    {"name":"ARKW", "url":"https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv"},
    {"name":"ARKG", "url":"https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_GENOMIC_REVOLUTION_MULTISECTOR_ETF_ARKG_HOLDINGS.csv"},
    {"name":"ARKF", "url":"https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv"},
    {"name":"ARKX", "url":"https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_SPACE_EXPLORATION_&_INNOVATION_ETF_ARKX_HOLDINGS.csv"},
    {"name":"PRNT", "url":"https://ark-funds.com/wp-content/fundsiteliterature/csv/THE_3D_PRINTING_ETF_PRNT_HOLDINGS.csv"}
      ]

for item in etfs:
    print('start...' + item['name'])

    request_headers = { 'User-Agent' : ('Mozilla/5.0 (Windows NT 10.0;Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'), }

    s=requests.get(item['url'], headers = request_headers).content

    df=pd.read_csv(io.StringIO(s.decode('utf-8')))

    # drop NAN data
    df.dropna(subset=['ticker'], inplace=True)

    # rename column for insert table
    df.rename(columns = {'market value($)': 'market_value'}, inplace = True)
    df.rename(columns = {'weight(%)': 'weight'}, inplace = True)

    # convert datetime to string date
    df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y').apply(lambda x: x.strftime('%Y%m%d'))
    df.tail()

    df.reset_index(drop=True, inplace=True)

    try:
        get_date = df.loc[df.index[0], 'date']

        df.to_sql(name='ark_etf_daily_list', con=engine, if_exists='append', index=False)
        print("##### " + get_date + ' - '+ item['name'] )
    except exc.IntegrityError:
        print("##### " + get_date + ' - '+ item['name'] + ' already exists' )
        pass
    yesterday = yesterday = date.today() - timedelta(days = 1)
    df.to_csv('/App/ark_etf/ark_data/'+item['name']+'-'+str(yesterday)+'.csv')
    df = pd.DataFrame()
    print('end...' + item['name'])
