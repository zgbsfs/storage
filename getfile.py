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
#	print string
	return string
def parseMeta(local,keyname):
	print 'in parse'
	Split = keyname.split('/')
	indexofthing = 'notfound'
	indexofbinthing ='notfound' 
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
	Tarf=[]
	Tarf.append(True)
	with tarfile.open(local, "r:gz") as tar:
		memberlist =tar.getmembers()
		for memberindex in  range(len(memberlist)):
			member = memberlist[memberindex]
			filenameintar =  str(member).split('\'')[1]
			if 'bin' in filenameintar:
				fileintar = tar.extractfile(filenameintar)
	                        content =pickle.load(fileintar)			
				if Tarf[0]:
					try:
						resultarr = [i for i, word in enumerate(content) if word.find(Split[1])!=-1]
						if resultarr:
							indexofbinthing=resultarr[0]
						else:
							continue
					except:
						continue
				else:
					try:
						indexofbinthing = content.index(thing)
					except:
						continue
				objoncloud = filenameintar.split('.')[0]
				member = memberlist[memberindex+1]
				filenameintar =  str(member).split('\'')[1]
				fileintar = tar.extractfile(filenameintar)
				content =pickle.load(fileintar)
				if indexofbinthing==0:
					binhead=0
				else:
					binhead=content[indexofbinthing-1]
				bintail=content[indexofbinthing]
				return binhead,bintail,objoncloud,Tarf
			if Split[1] in filenameintar:
				fileintar = tar.extractfile(filenameintar)
	                        content =pickle.load(fileintar)
				try:
					resultarr = [i for i, word in enumerate(content) if word.find(Split[2])!=-1]
					indexofthing = resultarr[0]
					thing= '/'+filenameintar.split('.')[0]
					member = memberlist[memberindex+1]
					filenameintar =  str(member).split('\'')[1]
					fileintar = tar.extractfile(filenameintar)
					content =pickle.load(fileintar)
					if indexofthing==0:
						filehead=0
					else:
						filehead=content[indexofthing-1]
					filetail=content[indexofthing]
					Tarf[0]=False
					Tarf.append(filehead)
					Tarf.append(filetail)
				except:
					continue
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
	if dest=='.':
		dest = keyname.replace('/','_')
	temp = keyname.replace('/',"_")
	'''direct download here'''
	#s3.meta.client.download_file(bucketname, keyname, keyname.split('/')[0]+'Meta.tar.gz')
	localmetafile = GetMetadata(bucketname,keyname,dest)
#	localmetafile=dest+'Meta.tar.gz'
	binhead,bintail,objoncloud,Tarf = parseMeta(localmetafile,keyname)
	print binhead
	print bintail
	print objoncloud
	print Tarf
	client = boto3.client('s3')
	if Tarf[0]:
		response = client.get_object(Bucket=bucketname,Key=objoncloud,Range='bytes='+str(binhead)+'-'+str(bintail-1))
		res_content = response['Body'].read()
		with open(temp,'wb') as newfile:
			newfile.write(res_content)
		newfile.close()
		with tarfile.open(temp, "r:gz") as tar:
			tar.extractall()
		tar.close
		print 'download the tar file please extract: ' +temp
	else:
		filehead = Tarf[1]
		filetail = Tarf[2]
		response = client.get_object(Bucket=bucketname,Key=objoncloud,Range='bytes='+str(filehead+binhead)+'-'+str(filetail+binhead-1))
		res_content = response['Body'].read()
		with open(dest,'wb') as newfile:
                        newfile.write(res_content)
                newfile.close()
		print 'download the file name :' +dest
#	print response['Body'].read()
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
