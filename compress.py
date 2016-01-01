from shutil import copyfileobj
import magic
import gzip
import os
import os.path
import tarfile
import sys
import pickle
import time

MAX_SIZE = 5 * 1000 * 1000
#Threshold = 30
MB = 1024*1024

def Combine_files(combine_list,sourceDir,WaitingToUpload,FileCounter):
	CombinedFileName = sourceDir.replace(sourceDir,WaitingToUpload)+str(FileCounter)+"_data"
	with open(CombinedFileName, 'wb') as outfile:
		for fname in combine_list:
#			if 'fffff.tar.gz' in fname:
#				print 'WTF in ' +str(FileCounter)
			with open(fname,'r') as infile:
				outfile.write(infile.read())
#				os.remove(fname)
	return CombinedFileName 

def Metadata(MetaFileInfo,MetaSizeInfo,NewFileName):
	MetaName = NewFileName+".meta"
	with open(MetaName, 'wb') as f:
		pickle.dump(MetaFileInfo+MetaSizeInfo, f)
	return NewFileName

def BigFileMetadata(Metafile,WaitingToUpload):
	BigMeta = WaitingToUpload+"Big.meta"
	with open(BigMeta, 'w') as f:
		pickle.dump(Metafile,f)

def Metadata_list(Dirpath):
	MetadataInDir = []
        totalsize = 0
        size_list =[]
	for (sourceDir, dirname, filename) in os.walk(Dirpath):
                for f in filename:
                        if ".meta" in f:
				metaindir = os.path.join(sourceDir, f)
                                MetadataInDir.append(metaindir)
			#	totalsize += os.path.getsize(metaindir)
			#	size_list.append(totalsize)
				
	return MetadataInDir #, size_list



def Compress_all_files(Dirpath):
	CompressfilesInDir = []
	totalsize = 0
        size_list =[]
	for (sourceDir, dirname, filename) in os.walk(Dirpath):
                for f in filename:
			if ".meta" not in f :
				fileindir = os.path.join(sourceDir, f)
				CompressedFile = fileindir+".gz"
				CompressfilesInDir.append(CompressedFile)
				with open(fileindir, 'rb') as input:
				    with gzip.open(CompressedFile, 'wb') as output:
					    copyfileobj(input, output)
	#			totalsize += os.path.getsize(CompressedFile)
	#			size_list.append(totalsize)
				
	return CompressfilesInDir #,size_list

def CompressBigFile(BigFilePath,S3KeyName,rootDir,WaitingToUpload,InDir):
	Formatlist = ['compress','video','audio','jpeg','mpeg','gzip','zip']
	print str(magic.from_file(BigFilePath,mime=True))
	for i in Formatlist:
		if  i in str(magic.from_file(BigFilePath,mime=True)):
			if InDir:
				print rootDir
				print S3KeyName
				print str(i)+ " WTF  "  + BigFilePath.replace(rootDir,S3KeyName)

				return os.path.abspath(BigFilePath),BigFilePath.replace(rootDir,S3KeyName)
			else:
				return os.path.abspath(BigFilePath),True
	CompressedFile = BigFilePath.replace(rootDir, WaitingToUpload) +".gz"
	CompressedMetaPath = BigFilePath.replace(rootDir,S3KeyName)+".gz"
	#print os.path.abspath(CompressedFile)
	with open(BigFilePath, 'rb') as input:
	    with gzip.open(CompressedFile, 'wb') as output:
        	copyfileobj(input, output)
	if InDir:
		return os.path.abspath(CompressedFile),CompressedMetaPath
	else:
		return os.path.abspath(CompressedFile),False


def MakeDir(directory):
	if not os.path.exists(directory):
		    os.makedirs(directory)

def Compression(rootDir,S3KeyName,Threshold,compression):
	combine_list = []
	BigFileList = []
	UploadBigFileList = []
	size_list = []
	NewFileName=[]
	WriteToBigFileMetaList=[]
	WriteToMeta_list=[]
	dirsize=0
	Combine_size=0
	FileCounter = 0
	Uploadsize=0	
	WaitingToUpload = os.getcwd()+S3KeyName 
	MakeDir(WaitingToUpload)
	localtime = time.asctime( time.localtime(time.time()) )
	print "Local current time :", localtime
	for ( sourceDir, dirname, filename) in os.walk(rootDir):
		#print os.path.getsize(sourceDir)/(1024.0)
		for f in filename:
		    sourcepath =  os.path.join(sourceDir, f)
		    if  Combine_size < Threshold * MB:
			    if os.path.getsize(sourcepath) > Threshold * MB:
				localtime = time.asctime( time.localtime(time.time()) )
				print "Local current time :", localtime
				#big file list
				Uploadsize += os.path.getsize(sourcepath)
				Newpath , Metapath = CompressBigFile(sourcepath,S3KeyName,rootDir,WaitingToUpload,True)
				UploadBigFileList.append(Newpath)
			        BigFileList.append(Metapath)
				WriteToBigFileMetaList.append(Metapath.replace(rootDir,S3KeyName))
			    else:
				#small file connect    
				Combine_size+=os.path.getsize(sourcepath)
				combine_list.append(sourcepath)
				WriteToMeta_list.append(sourcepath.replace(rootDir,S3KeyName))
				size_list.append(Combine_size)
				#print total_sizste
		    else:
			FileCounter = FileCounter +1
			print FileCounter
			CombinedFileName = Combine_files(combine_list,rootDir,WaitingToUpload,FileCounter)
			Newpath = os.path.abspath(Metadata(WriteToMeta_list,size_list,CombinedFileName))
			NewFileName.append(Newpath)
			Uploadsize += Combine_size
			Combine_size=0
			combine_list=[]
			WriteToMeta_list=[]
			size_list=[]
	#break the for loop ,combined remaining files
	if Combine_size >0:
		Uploadsize += Combine_size
		FileCounter = FileCounter +1

		CombinedFileName = Combine_files(combine_list,rootDir,WaitingToUpload,FileCounter)
		Newpath = os.path.abspath(Metadata(WriteToMeta_list,size_list,CombinedFileName))
		NewFileName.append(Newpath)	

	if BigFileList: 
		BigFileMetadata(BigFileList,WaitingToUpload)
	b=0
	Metadatalist     = Metadata_list(WaitingToUpload)

	if not compression:
		for f in NewFileName+UploadBigFileList+Metadatalist:
			b+=os.path.getsize(f)
		print "== FAlse"
		return NewFileName+UploadBigFileList,Metadatalist,b
	else:
		Compressfilelist = Compress_all_files(WaitingToUpload)


		for f in UploadBigFileList+Compressfilelist:
			b+=os.path.getsize(f)
		print "== TRUe"
		return UploadBigFileList+Compressfilelist,Metadatalist ,b
	'''
	Combine_files(Compressfilelist,rootDir,WaitingToUpload,"upload")
	print combine_size_list
	print '\n'
	Metafilelist ,combine_meta_size_list = Metadata_list(WaitingToUpload)
	Combine_files(Metafilelist,rootDir,WaitingToUpload,"Meta")
	print combine_meta_size_list
	print '\n'
	'''
