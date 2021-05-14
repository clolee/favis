#!/home/changlo/anaconda3/bin/python
import requests
import datetime
import sqlite3
import bs4 
# user define package import
import sys
sys.path.append("/App/favis")
from msgbot.favisbot import favisbot
import util.favis_util as favis_util
from util.favis_logger import FavisLogger
import pymysql

# set config
task_id = 'official_notice'
logger = FavisLogger(task_id, task_id + '_' + datetime.datetime.today().strftime('%Y%m%d'))
###################################################################################
logger.info('\n\n' + str(datetime.datetime.today()) + ' : ' + task_id + ' start...')
starttime = datetime.datetime.now()

conn = favis_util.get_favis_mysql_connection()
cur = conn.cursor()
bot = favisbot()
search_date = (datetime.datetime.now().strftime('%Y%m%d')).replace('-', '.')
print(search_date)
exit
#search_date = '20160511'
groups = ['Y','K'] # 유가증권, 코스닥 구분

for group in groups:
	# find start, end page number
#	url = "http://dart.fss.or.kr/dsac001/search.ax"
	url = "http://dart.fss.or.kr/dsac001/mainY.do"
	formdata = {'selectDate':search_date,'currentPage':1, 'pageGrouping':group, 'mdayCnt':0}
	data = requests.post(url, formdata)    
	data = bs4.BeautifulSoup(data.text, "html5lib")
	pageinfo = data.find("p", attrs={'class':'page_info'}).text.strip().split(']')[0].split('[')[1].split('/')
	endpage = int(pageinfo[1])
	startpage = int(pageinfo[0])

	cnt = 0
	for i in range(startpage, endpage + 1):
		market_type = ''
		if group == 'K':
			market_type = 'KOSDAQ'
		elif group == 'Y':
			market_type = 'KOSPI'
			
		url = "http://dart.fss.or.kr/dsac001/mainY.do"
		formdata = {'selectDate':search_date,'currentPage':i, 'pageGrouping':group, 'mdayCnt':0}
		data = requests.post(url, formdata)    

		data = bs4.BeautifulSoup(data.text, "html5lib")
		#articles = data.findAll("table", attrs={'class':'view'})
		notices = data.findAll("tr")
		header = notices[0]

		for i in range(1,len(notices)):
			tds = notices[i].findAll("td")
			time = str(tds[0].text.strip())
			link = str('http://dart.fss.or.kr' + tds[2].find('a')['href'])
			title = str(tds[2].find('a').text.strip())

			company = str(tds[3].text.strip())
			regdate = str(tds[4].text.strip()).replace('.', '')

			try:
				logger.debug (regdate + ',' + time + ',' + title + ',' + link + ',' + company + ',' + market_type)
#				bot.whisper('plain','[유상증자 관련 공시]\n공시일시: '+regdate+" "+time+"\n회사: "+company+"("+market_type+")\n공시내용: "+title+ "\n상세링크: "+link)				

				cur.execute("INSERT INTO  official_notice (date, time, title, link, company, market_type) VALUES (%s, %s, %s, %s, %s, %s)", 
				(regdate, time, title, link, company, market_type))
				conn.commit()
			except pymysql.IntegrityError:
				pass
			except pymysql.Error as e:
				if conn:
					conn.rollback()
				logger.error ("error %s" % e.args[0])
			cnt = cnt + 1


query = "select * from official_notice "
#query += "where date = '"+search_date+"' "
#query += "where date = '20210402' "
query += "where date = DATE_FORMAT(NOW(), '%Y%m%d') "
query += "AND (TIME_FORMAT(TIME, '%H:00') >= HOUR(NOW() - interval 1 HOUR) AND TIME_FORMAT(TIME, '%H:00') < HOUR(NOW()))  "
query += "and (title like '%무상증자%' or title LIKE '%유상감자%' or title LIKE '%액면분할%'  or title LIKE '%자기주식%' or title like '%배정%') "
query += "order by time asc"

logger.debug(query)
cur.execute(query)



rows = cur.fetchall()
for row in rows:
	logger.debug(row)
	bot.whisper('plain','[주요 공시]\n공시일시: ' + row['date']+" "+row['time']
				+"\n회사: "+row['company']+"("+row['market_type']+")\n공시내용: "+row['title'] + "\n상세링크: "+row['link'])

if conn:
    conn.close()

endtime = datetime.datetime.now()
logger.info('count :' + str(cnt) + ', bot send :' + str(len(rows)) + ', elaspsedtime : ' + str(endtime - starttime))
logger.info(str(datetime.datetime.today()) + ' : ' + task_id + ' end...')


