import pymysql
import logging
import logging.handlers
import datetime, time

def get_favis_mysql_connection():
	try:
		conn = pymysql.connect(host='localhost',
							 user='root',
							 password='ckdfh76!!',
							 db='favis',
							 charset='utf8mb4',
							 cursorclass=pymysql.cursors.DictCursor)
		return conn
	except pymysql.Error as e:
		if conn:
			conn.close()
		print ("error %s" % e.args[0])	


def setLogger(id, filename):
	# set config
	favis_log_path='/app/favis/logs/'
	# 로거 인스턴스를 만든다
	logger = logging.getLogger(id)

	# 포매터를 만든다
	fomatter = logging.Formatter('[%(levelname)s][%(asctime)s] : %(message)s')

	# 스트림과 파일로 로그를 출력하는 핸들러를 각각 만든다.
	#fileMaxByte = 1024 * 1024 * 100 #100MB
	#fileHandler = logging.handlers.RotatingFileHandler(favis_log_path + filename + '.log', maxBytes=fileMaxByte, backupCount=10)
	fileHandler = logging.FileHandler(favis_log_path + filename + '.log')
	# 로거 인스턴스에 스트림 핸들러와 파일핸들러를 붙인다.
	logger.addHandler(fileHandler)
	logger.setLevel(logging.DEBUG)
	return logger


def daterange(start, end):
	start_date = datetime.datetime.strptime(start, '%Y%m%d').date()
	end_date = datetime.datetime.strptime(end, '%Y%m%d').date()

	return (start_date + datetime.timedelta(days=i) for i in range((end_date - start_date).days + 1))


