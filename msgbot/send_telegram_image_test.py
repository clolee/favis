#!/usr/bin/python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


x=range(100)
y=[i*i for i in x]
plt.plot(x,y)
plt.savefig('aaa.png', bbox_inches='tight')


from msgbot.favisbot import favisbot
bot = favisbot()
bot.whisper('plain','hahaha')

import telegram
favisbot = telegram.Bot(token='201142916:AAEBvuAYEXCKFe6Ql_DdkBk6V3Y3G6CdIZU')
chat_id = 185388733
# post image file from disk
favisbot.sendPhoto(chat_id=chat_id, photo=open('aaa.png', 'rb'))
