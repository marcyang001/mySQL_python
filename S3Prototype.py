
"""
template for S3 storage



"""

from enum import Enum
import os
import boto
import time
from boto.s3.connection import Location
from boto.s3.connection import S3Connection


class DataType(Enum):
	file = 1
	stringwords = 2


class S3Transfer(object):
	

	def __init__(self, accountID=None, secretKey=None):
		
		#temporary storage for recording the uploaded files 
		#In practice, we should use a database to store the information
		self.uploadRecord = {}
		try:
			if accountID == None and secretKey == None:
				self.conn = boto.connect_s3()
			else:
				self.conn = S3Connection(accountID, secretKey)			
		except:
			raise Exception("no authentication found in .aws directory")
		

	def createBucket(self, name, location=None):
		
		currentDate = time.strftime("%d-%m-%Y")
		name = name+"-" +currentDate
		
		if location != None and location.isupper() in [i.isupper() for i in dir(Location)]:
			bucket = self.conn.create_bucket(name, location=location) 
		elif location != None:
			bucket = self.conn.create_bucket(name, location=default)
		else:
			bucket = self.conn.create_bucket(name)
	

	
	def storingData(self, bucket, keyValue, File, datatype=DataType.file, override=False):
		"""
			This is only a simple way to store data
			There can be another way of storing: breaking into chunks
		"""
		
		b = self.conn.get_bucket(bucket)

		for key in b:
			if key.name == keyValue and override == False:
				raise Exception("WARNING: Same key in the same bucket.")


		from boto.s3.key import Key
		k = Key(b)
		k.key = keyValue

		if datatype == DataType.file:
			k.set_contents_from_filename(File)
		else:
			k.set_contents_from_string(File)

		#keep track the files uploaded
		self.uploadRecord[File] = [str(keyValue), k.etag]  

		return k.etag


	
	def downloadData(self, bucket=None, fileKey=None):
		"""
			Download a specific file from a specific bucket,
			otherwise, download all the file from the specified bucket
		"""
		
		b = self.conn.get_bucket(bucket)
		bucket_list = b.list()
		
		#downloading a specific file
		if fileKey != None:	
			for f in bucket_list:
				keyString = str(f.key)
				if fileKey == keyString:
					f.get_contents_to_filename(fileKey)
					return
			raise Exception("File name not found")
		else:
			#download the whole bucket
			for l in bucket_list:
				keyString = str(l.key)
				# check if file exists locally, if not: download it
				l.get_contents_to_filename(keyString)


	
	def retrieveETag(self, bucket=None, fileKey=None, validate=True):
		"""
			return a list of files with etags or the etag associated to the specific file
			raise exception for error handling

		"""

		etagList = {}
		mybucket = None
		
		from boto.s3.key import Key
		
		if bucket != None:
			try:
				mybucket = self.conn.get_bucket(bucket, validate=validate)
				if fileKey != None:
					if fileKey in mybucket:
						key = mybucket.get_key(fileKey)
						etagList[fileKey] = key.etag
				else:
					#retrieve all etags in the bucket
					for keys in mybucket:
						etagList[str(keys.name)] = keys.etag

			except boto.exception.S3ResponseError as e:
				error_code = int(e[0])
				if error_code == 404:
					print "WARNING: bucket not exist"
				return {}

		elif bucket == None and fileKey != None:
			raise Exception("WARNING: Bucket is Empty")

		else:
			#retrieve all files from every bucket
			for bucket_item in self.conn:
				for key in bucket_item:
					etagList[str(key.name)] = [key.etag, str(bucket_item.name)]


		return etagList


	def verifyFiles(self, bucket=None, File=None, localDir=None):
		
		"""
			use MD5 to verify consistency of the files between local server and Amazon S3
			raise exception
		"""
		import hashlib

		localFile = None
		if localDir != None and File != None:
			localFile = str(localDir) + "/" + str(File)


		consistent = False
	
		etagList = self.retrieveETag(bucket=bucket, fileKey=File)

		if bool(etagList) and File != None:

			BLOCKSIZE = 65536
			hasher = hashlib.md5()
			with open(File, 'rb') as afile:
					
				buf = afile.read(BLOCKSIZE)
				while len(buf) > 0:
					hasher.update(buf)
					buf = afile.read(BLOCKSIZE)


			md5code = hasher.hexdigest()
			etaginS3 = etagList[File]

			if md5code == etaginS3.strip("\"\""):
				consistent = True
			

		return consistent


	def deleteUploadedFilesInDir(self, path=os.getcwd()):

		"""
		delete the uploaded files in the current directory

		"""
			
		fileList = []

		print self.uploadRecord

		for fileName, properties in self.uploadRecord.iteritems():
 			print fileName
 			os.remove(path+"/"+ fileName)
 			fileList.append(fileName)

		for files in fileList:
			del self.uploadRecord[files]

		






mys3 = S3Transfer()

print mys3.verifyFiles(bucket="mybucketcreatedfrompython", File='hello.txt', localDir="~/S3Prototype")
#mys3.storingData("mybucketcreatedfrompython", "hello.txt", "hello.txt", DataType.file, True)
#mys3.storingData("mybucketcreatedfrompython", "foobar", "foobar", DataType.file, True)
#mys3.storingData("mybucketcreatedfrompython", "foodoc", "foodoc", DataType.file, True)

#mys3.downloadData("mybucketcreatedfrompython")
#mys3.deleteUploadedFilesInDir()



