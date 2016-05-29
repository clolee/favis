#!/usr/bin/python
import requests
import datetime
import sqlite3
import bs4 
# user define package import
import sys
sys.path.append("/app/favis")
from msgbot.favisbot import favisbot

search_date = datetime.datetime.now().strftime('%Y%m%d')
#search_date = '20160511'
groups = ['K','Y'] # 유가증권, 코스닥 구분

for group in groups:
    # find start, end page number
    url = "http://dart.fss.or.kr/dsac001/search.ax"
    formdata = {'selectDate':search_date,'currentPage':1, 'pageGrouping':group}
    data = requests.post(url, formdata)    
    data = bs4.BeautifulSoup(data.text, "html5lib")
    pageinfo = data.find("p", attrs={'class':'page_info'}).text.strip().split(']')[0].split('[')[1].split('/')
    endpage = int(pageinfo[1])
    startpage = int(pageinfo[0])
    
    for i in range(startpage, endpage + 1):
        market_type = ''
        if group == 'K':
            market_type = 'KOSDAQ'
        elif group == 'Y':
            market_type = 'KOSPI'
            
        url = "http://dart.fss.or.kr/dsac001/search.ax"
        formdata = {'selectDate':search_date,'currentPage':i, 'pageGrouping':group}
        data = requests.post(url, formdata)    

        data = bs4.BeautifulSoup(data.text, "html5lib")
        #articles = data.findAll("table", attrs={'class':'view'})
        notices = data.findAll("tr")
        header = notices[0]

        datalist = []
        for i in range(1,len(notices)):
            tds = notices[i].findAll("td")
            time = tds[0].text.strip()
            link = 'http://dart.fss.or.kr' + tds[2].find('a')['href']
            title = tds[2].find('a').text.strip()

            company = tds[3].text.strip()
            regdate = tds[4].text.strip()

            datalist.append((search_date, time, title, link, company, market_type, regdate))

        try:
            con = sqlite3.connect('officialnotice.db')

            with con:
                cur = con.cursor()
                cur.execute('create table if not exists official_notice ( date TEXT, time TEXT, title TEXT, link TEXT, compnay TEXT, market_type TEXT, regdate TEXT)')
                cur.execute('CREATE UNIQUE INDEX if not exists data_idx ON official_notice (date, time, link)')

                cur.executemany("INSERT OR REPLACE INTO  official_notice VALUES (?, ?, ?, ?, ?, ?, ?)", datalist)
                con.commit()
        except sqlite3.Error as e:
            if con:
                con.rollback()
            print ("error %s" % e.args[0])

        finally:
            if con:
                con.close()

bot = favisbot()

try:
    con = sqlite3.connect('officialnotice.db')

    with con:
        cur = con.cursor()
        cur.execute("select * from official_notice where date = '"+search_date+"' and title like '%증자%' or title like '%배정%'")

        rows = cur.fetchall()
        for row in rows:
            print ("%s|%s|%s|%s|%s|%s|%s " %  (row[0], row[1], row[2], row[3], row[4], row[5], row[6]) )
            bot.whisper('plain','[유상증자 관련 공시]'
                        +'\n공시일시: ' + row[0]+" "+row[1]
                        +"\n회사: "+row[4]+"("+row[5]+")"
                        +"\n공시내용: "+row[2]
                        +"\n상세링크: "+row[3])

except sqlite3.Error as e:
    if con:
        con.rollback()
    print ("error %s" % e.args[0])

finally:
    if con:
        con.close()                
                
