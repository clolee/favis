# -*- coding: utf-8 -*-

import pymysql
import logging
import logging.handlers
import datetime, time
import pandas as pd

def get_favis_mysql_connection():
	try:
		conn = pymysql.connect(host='192.168.0.6',
							 user='mnilcl',
							 password='Cloud00!',
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
	# �ΰ� �ν��Ͻ��� �����
	logger = logging.getLogger(id)

	# �����͸� �����
	fomatter = logging.Formatter('[%(levelname)s][%(asctime)s] : %(message)s')

	# ��Ʈ���� ���Ϸ� �α׸� ����ϴ� �ڵ鷯�� ���� �����.
	#fileMaxByte = 1024 * 1024 * 100 #100MB
	#fileHandler = logging.handlers.RotatingFileHandler(favis_log_path + filename + '.log', maxBytes=fileMaxByte, backupCount=10)
	fileHandler = logging.FileHandler(favis_log_path + filename + '.log')
	# �ΰ� �ν��Ͻ��� ��Ʈ�� �ڵ鷯�� �����ڵ鷯�� ���δ�.
	logger.addHandler(fileHandler)
	logger.setLevel(logging.DEBUG)
	return logger



def getDateRangeList(start, end):
	start_date = datetime.datetime.strptime(start, '%Y%m%d').date()
	end_date = datetime.datetime.strptime(end, '%Y%m%d').date()

	return [(start_date + datetime.timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date - start_date).days + 1)]


def getWorkingDays(start, end):
	return[d.strftime('%Y%m%d') for d in pd.date_range(start, end, freq='B')]
