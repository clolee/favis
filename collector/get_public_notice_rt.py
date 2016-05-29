#!/usr/bin/python

import requests
import time
import bs4 # beautifulsoup
# user define package import
import sys
sys.path.append("/app/favis")
from msgbot.favisbot import favisbot

bot = favisbot()

'''@param gmtstr'''
def gmtstr_to_kststr(gmtstr, gmtstr_format, kststr_format):
    # convert kst (gmt + 9 hour)
    dt = time.mktime(time.strptime(gmtstr, gmtstr_format)) + (60 * 60 * 9)
    return time.strftime(kststr_format,time.localtime(dt))

# DART realtime official notice rss feed url
url = 'http://dart.fss.or.kr/api/todayRSS.xml'

data = requests.get(url)
data = bs4.BeautifulSoup(data.text, 'lxml')
# extract item list
notices = data.findAll("item")

for notice in notices:
    title = notice.find('title').string
    link =  notice.find('guid').string
    regdate = gmtstr_to_kststr(notice.find('dc:date').string, '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S')
    
    search_keywords = ['증자','유상']
    if any(sub in title for sub in search_keywords):
        print('[유상증자 관련 공시]'
                     +'\n공시일시 : ' + regdate
                     +"\n공시내용 : "+title
                     +"\n상세링크 : "+link)
        
        bot.whisper('plain','[유상증자 관련 공시]'
                      +'\n공시일시 : ' + regdate
                      +"\n공시내용 : "+title
                      +"\n상세링크 : "+link)


