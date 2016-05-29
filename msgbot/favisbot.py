# https://github.com/python-telegram-bot/python-telegram-bot
# sudo pip install python-telegram-bot
import telegram

class favisbot(object):
    '''favisbot is communicate with telegram'''

    def __init__(self, *args):
        # if not args:
        #     print ('no arguments')
        # else:
        #     for i,arg in enumerate(args):
        #         if type(arg)==str:
        #             print (arg)
        self.favisbot = telegram.Bot(token='201142916:AAEBvuAYEXCKFe6Ql_DdkBk6V3Y3G6CdIZU')
        self.chat_id = 185388733

    def setBotKey(self, token, chat_id):
        self.favisbot = telegram.Bot(token=token)
        self.chat_id = chat_id

    def whisper(self, msgtype, msg):
        if not msgtype:
            print ("msgtype is not allowed null or empty. only use in [plain|html|imglink|img]")

        bot = self.favisbot
        if msgtype == 'plain':
              bot.sendMessage(chat_id=self.chat_id, text=msg)
        elif msgtype == 'html':
            bot.sendMessage(chat_id=self.chat_id, text=msg, parse_mode=telegram.ParseMode.HTML)
        elif msgtype == 'imglink':
            bot.sendPhoto(chat_id=self.chat_id, photo=msg)
        elif msgtype == 'img':
            bot.sendPhoto(chat_id=self.chat_id, photo=open(msg, 'rb'))
        else:
            print ("msgtype is not allowed except [plain|html|imglink|img]")
            

        
if __name__ == '__main__':
     bot = favisbot()
     bot.whisper('plain','testing')
        
    
