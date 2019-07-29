#!/App/tools/anaconda3/bin/python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime
import pandas as pd
from pandas import DataFrame
# user define package import
# user define package import
favis_path = "/app/favis/"
import sys
sys.path.append(favis_path)
from msgbot.favisbot import favisbot
import util.favis_util as favis_util
import pymysql
# set config
###################################################################################
conn = pymysql.connect(host='192.168.10.18',
                         user='mnilcl',
                         password='Cloud00!',
                         db='favis',
                         charset='utf8mb4',
                         cursorclass=pymysql.cursors.DictCursor)

cur = conn.cursor()
stock_code = '009180'
query = "select i.date, MAX(CASE WHEN ii.name = si.market THEN i.close END) as index_close \
        ,  d.code, d.close as stock_close, d.volume, t.foreigner, t.institution \
         from daily_index i, daily_info d, trading_trend t, stock_info si, index_info ii\
        where i.code in ('KS11', 'KQ11') and i.code = ii.code and i.date > '20160101' and i.date = d.date  and i.date = t.date and d.code = t.code \
        and d.code = '"+stock_code+"' and d.code = si.code GROUP BY i.date, d.code, d.close, d.volume, t.foreigner, t.institution order by date asc"
cur.execute(query)

df = pd.read_sql(query, conn)
df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
df = df.set_index('date')

if conn:
    conn.close()

#df['MA_5'] = pd.stats.moments.rolling_mean(df['Adj Close'], 5)
df['MA_5'] = df['stock_close'].rolling(5).mean()
df['MA_20'] = df['stock_close'].rolling(20).mean()
df['diff'] = df['MA_5'] - df['MA_20']

fig = matplotlib.pyplot.gcf()
fig.set_size_inches(16, 26)

# price (가격)
price_chart = plt.subplot2grid((5,1), (0, 0), rowspan=1)
price_chart.plot(df.index, df['stock_close'], label='stock_close', lw=3)
price_chart.plot(df.index, df['MA_5'], label='MA 5day' , lw=2)
price_chart.plot(df.index, df['MA_20'], label='MA 20day', lw=1)
price_chart.grid(True)

plt.title(stock_code )
plt.legend(loc='best')

# 지수 (포인트)
index_chart = plt.subplot2grid((5,1), (1,0), rowspan=1)
index_chart.plot(df.index, df['index_close'], label='index_close', lw=1, color='r')
index_chart.set_title('Index')
index_chart.grid(True)

# volume (거래량)
vol_chart = plt.subplot2grid((5,1), (2,0), rowspan=1)
vol_chart.bar(df.index, df['volume'], color='c')
vol_chart.set_title('Volume')
vol_chart.grid(True)

# volume (거래량)
trend_chart = plt.subplot2grid((5,1), (3,0), rowspan=1)
trend_chart.bar(df.index, df['foreigner'], color='r')
trend_chart.bar(df.index, df['institution'], color='b')
trend_chart.set_title('Foreigner - INstitution')
trend_chart.grid(True)


# 이동평균의 차이
signal_chart = plt.subplot2grid((5,1), (4,0), rowspan=1)
signal_chart.plot(df.index, df['diff'].fillna(0), color='g')
plt.axhline(y=0, linestyle='--', color='k')
signal_chart.set_title('MA')
signal_chart.grid(True)

# sell, buy annotate
prev_key = prev_val = 0

# Sell/Buy 시그널 Annotation
def annote_signal(chart, xy, text):
    textcoords='offset points'
    arrowprops=dict(arrowstyle='-|>')
    if text == 'Buy':
        xytext=(10,-30)
    elif text == 'Sell':
        xytext=(10,30)
    else:
        return
    chart.annotate(text, xy=xy, xytext=xytext, textcoords=textcoords, arrowprops=arrowprops)

for key, val in df['diff'].iteritems():
    if val == 0:
        continue
    if val * prev_val < 0 and val > prev_val:
        annote_signal(signal_chart, (key, df['diff'][key]), 'Buy')
    elif val * prev_val < 0 and val < prev_val:
        annote_signal(signal_chart, (key, df['diff'][key]), 'Sell')
    prev_key, prev_val = key, val
          
for key, val in df['diff'].iteritems():
    if val == 0:
        continue
    if val * prev_val < 0 and val > prev_val:
        annote_signal(price_chart, (key, df['stock_close'][key]), 'Buy')
    elif val * prev_val < 0 and val < prev_val:
        annote_signal(price_chart, (key, df['stock_close'][key]), 'Sell')
    prev_key, prev_val = key, val
    
    
plt.savefig(stock_code + '.png', bbox_inches='tight')


bot = favisbot()
bot.whisper('img',stock_code + '.png')