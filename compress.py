from shutil import copyfileobj
import magic
import gzip
import os
import os.path
import tarfile
import sys
import pickle

MAX_SIZE = 5 * 1000 * 1000
#Threshold = 30
MB = 1024*1024

def Combine_files(combine_list,sourceDir,WaitingToUpload,FileCounter):
	CombinedFileName = sourceDir.replace(sourceDir,WaitingToUpload)+str(FileCounter)+"_data"
	with open(CombinedFileName, 'wb') as outfile:
		for fname in combine_list:
			with open(fname,'r') as infile:
				outfile.write(infile.read())
#				os.remove(fname)
	return CombinedFileName 

def Metadata(MetaFileInfo,MetaSizeInfo,NewFileName):
	MetaName = NewFileName+".meta"
	with open(MetaName, 'w') as f:
		pickle.dump(MetaFileInfo, f)
		pickle.dump(MetaSizeInfo, f)
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
			if ".meta" not in f:
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

	if  str(magic.from_file(BigFilePath,mime=True))=="video/mp4":
		return  BigFilePath
	CompressedFile = BigFilePath.replace(rootDir, WaitingToUpload) +".gz"
	CompressedMetaPath = BigFilePath.replace(rootDir,S3KeyName)+".gz"
	#print os.path.abspath(CompressedFile)
	with open(BigFilePath, 'rb') as input:
	    with gzip.open(CompressedFile, 'wb') as output:
        	copyfileobj(input, output)
	if InDir:
		return os.path.abspath(CompressedFile),CompressedMetaPath
	else:
		return os.path.abspath(CompressedFile)


def MakeDir(directory):
	if not os.path.exists(directory):
		    os.makedirs(directory)

def Compression(rootDir,S3KeyName,Threshold):
	destDir=''
	bucket_name='asdf'
	pairs = []
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
	
	for ( sourceDir, dirname, filename) in os.walk(rootDir):
		#print os.path.getsize(sourceDir)/(1024.0)
		for f in filename:
		    sourcepath =  os.path.join(sourceDir, f)
		    if  Combine_size < Threshold * MB:
			    if os.path.getsize(sourcepath) > Threshold * MB:
				#big file list
				Newpath , Metapath = CompressBigFile(sourcepath,S3KeyName,sourceDir+'/',WaitingToUpload,True)
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
			CombinedFileName = Combine_files(combine_list,rootDir,WaitingToUpload,FileCounter)
			Newpath = os.path.abspath(Metadata(WriteToMeta_list,size_list,CombinedFileName))
			NewFileName.append(Newpath)
			Uploadsize += Combine_size
			Combine_size=0
			combine_list=[]
			WriteToMeta_list=[]
			size_list=[]
	#break the for loop ,combined remaining files
	FileCounter = FileCounter +1
	CombinedFileName = Combine_files(combine_list,rootDir,WaitingToUpload,FileCounter)
	Newpath = os.path.abspath(Metadata(WriteToMeta_list,size_list,CombinedFileName))
	NewFileName.append(Newpath)	
	if BigFileList: 
		BigFileMetadata(BigFileList,WaitingToUpload)
	Compressfilelist = Compress_all_files(WaitingToUpload)
	Metadatalist     = Metadata_list(WaitingToUpload)
	return Compressfilelist,Metadatalist ,Uploadsize
	'''
	Combine_files(Compressfilelist,rootDir,WaitingToUpload,"upload")
	print combine_size_list
	print '\n'
	Metafilelist ,combine_meta_size_list = Metadata_list(WaitingToUpload)
	Combine_files(Metafilelist,rootDir,WaitingToUpload,"Meta")
	print combine_meta_size_list
	print '\n'
	'''
