import requests




class FileDownloader:

	def __init__(self, hostname, port):
		self.hostname = hostname
		self.port = port
		self.url = 'http://{}:{}/'.format(hostname, port)



	def getUrl(self):
		print self.url



fd = FileDownloader('mtl1-tj-dev', 1060)

fd.getUrl()








