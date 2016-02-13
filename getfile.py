import urlparse
from boto.s3.connection import S3Connection
import pickle
import boto3
import os
import tarfile
def longpath(keyname):
	Split = keyname.split('/')
	string=''
	for i in range(2,len(Split)-1):
		print i
		string +=Split[i]
		string +='_'
	string += Split[i+1]
	print string
	return string
def parseMeta(local,keyname):
	print 'in parse'
	Split = keyname.split('/')
	longSplit=[]
	if len(Split)==2:
		Split.append(Split[1])
		Split[1]='UnderRoot'
	elif len(Split)>3:
		longSplit.append(Split[0])
		longSplit.append(Split[1])
		longSplit.append(longpath(keyname))
		Split = longSplit
	print Split
	with tarfile.open(local, "r:gz") as tar:
		for tarinfo in tar:
			filenameintar =  str(tarinfo).split('\'')[1]
			if Split[1] in filenameintar:
#				print filenameintar
				fileintar = tar.extractfile(filenameintar)
				content =pickle.load(fileintar)
				try:	
					index = [i for i,word in enumerate(content) if word.find(Split[2])!=-1]
					print content[index]
					print index
				except:
					pass
					
			

def GetMetadata(bucketname,keyname,dest):
	s3 = boto3.resource('s3')
	downloadmeta = keyname.split('/')[0]+'/Meta.tar.gz'
	localmeta = dest+'Meta.tar.gz'
	s3.meta.client.download_file(bucketname, downloadmeta,localmeta)
	return os.path.abspath(localmeta)

def GetTheFile(Filepath,dest):
	print Filepath
#	print dest
	split_rs = urlparse.urlsplit(Filepath)
	print split_rs
	keyname = split_rs.path[1:]
	bucketname = split_rs.netloc

	'''direct download here'''
	#s3.meta.client.download_file(bucketname, keyname, keyname.split('/')[0]+'Meta.tar.gz')
	localmetafile = GetMetadata(bucketname,keyname,dest)
	parseMeta(localmetafile,keyname)
	return 
	client = boto3.client('s3')
	response = client.get_object(Bucket=bucketname,Key='1g/bin1',Range='bytes='+str(filehead+binhead)+'-'+str(filehead))
	print response['Body'].read()
	'''
	s3 = S3Connection()
	split_rs = urlparse.urlsplit(Filepath)
	bucket = s3.get_bucket(split_rs.netloc)
	s3key = '/'+split_rs.path.split('/')[1]+'/'

	key = bucket.get_key('1g/bin1')
	print key

	a = open('gettest','wb')
	key.get_contents_as_string(a,headers={'Range':'bytes=0-1000'})
	print b
	return
	print s3key
	print split_rs
	'''
