import urlparse
from boto.s3.connection import S3Connection
import pickle

def GetMetadata(MetaFileInfo,MetaSizeInfo,NewFileName):
	with open(MetaName, 'wb') as f:
		Thelist = pickle.load(f)
		return Thelist[:len(Thelist)/2],Thelist[len(Thelist)/2+1:]
def GetTheFile(Filepath):
	s3 = S3Connection()
	split_rs = urlparse.urlsplit(Filepath)
	bucket = s3.get_bucket(split_rs.netloc)
	s3key = '/'+split_rs.path.split('/')[1]+'/'
	key = bucket.get_key(s3key+'Meta_data')
	a = open('gettest','wb')
	key.get_contents_as_string(a,{'Range': 'bytes=0-100000'})
	
	print s3key
	print split_rs
