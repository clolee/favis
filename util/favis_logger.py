import logging
import logging.handlers

class FavisLogger:
	# set config
	favis_log_path='/app/favis/logs/'

	def __init__(self, id, filename):
		# 로거 인스턴스를 만든다
		self.logger = logging.getLogger(id)

		# 포매터를 만든다
		fomatter = logging.Formatter('[%(levelname)s][%(asctime)s] : %(message)s')

		# 스트림과 파일로 로그를 출력하는 핸들러를 각각 만든다.
		#fileMaxByte = 1024 * 1024 * 100 #100MB
		#fileHandler = logging.handlers.RotatingFileHandler(favis_log_path + filename + '.log', maxBytes=fileMaxByte, backupCount=10)
		fileHandler = logging.FileHandler(self.favis_log_path + filename + '.log')
		# 로거 인스턴스에 스트림 핸들러와 파일핸들러를 붙인다.
		self.logger.addHandler(fileHandler)
		self.logger.setLevel(logging.DEBUG)
	
	def debug(self, msg):
		self.logger.debug(msg)

	def info(self, msg):
		self.logger.info(msg)


if __name__ == '__main__':
	fl =  FavisLogger('test','test')
	fl.debug('hi')
        
        
        
        