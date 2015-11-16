import gzip
import os
import os.path
import tarfile
import sys
import pickle

MAX_SIZE = 5 * 1000 * 1000
Threshold = 50
MB = 1024*1024

def Combine_files(combine_list,sourceDir,WaitingToUpload,FileCounter):
	CombinedFileName = sourceDir.replace(sourceDir,WaitingToUpload)+str(FileCounter)+"_data"
	with open(CombinedFileName, 'w') as outfile:
		for fname in combine_list:
			with open(fname) as infile:
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


def CompressBigFile(BigFilePath,S3KeyName,rootDir,WaitingToUpload,InDir):

	CompressedFile = BigFilePath.replace(rootDir, WaitingToUpload) +".compressed"
	data = open(BigFilePath, 'r').read()

	output = gzip.open(CompressedFile, 'wb')
	try:
		output.write(data)
	finally:
		output.close()
#		os.remove(BigFilePath)
	if InDir:
		return os.path.abspath(CompressedFile),CompressedFile
	else:
		return os.path.abspath(CompressedFile)


def MakeDir(directory):
	if not os.path.exists(directory):
		    os.makedirs(directory)

def Compression(rootDir,S3KeyName):
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
	total_size=0
	FileCounter = 0
	
	WaitingToUpload = os.getcwd()+S3KeyName
	MakeDir(WaitingToUpload)
	for ( sourceDir, dirname, filename) in os.walk(rootDir):
		#print os.path.getsize(sourceDir)/(1024.0)
		for f in filename:
		    sourcepath =  os.path.join(sourceDir, f)
		    if  total_size < Threshold * MB:
			    if os.path.getsize(sourcepath) > Threshold * MB:
				#big file list
				Newpath , Metapath = CompressBigFile(sourcepath,S3KeyName,rootDir,WaitingToUpload,True)
				UploadBigFileList.append(Newpath)
			        BigFileList.append(Metapath)
				WriteToBigFileMetaList.append(Metapath.replace(rootDir,S3KeyName))
			    else:
				#small file connect    
				total_size+=os.path.getsize(sourcepath)
				combine_list.append(sourcepath)
				WriteToMeta_list.append(sourcepath.replace(rootDir,S3KeyName))
				size_list.append(total_size)
				#print total_size
		    else: 
			FileCounter = FileCounter +1
			CombinedFileName = Combine_files(combine_list,rootDir,WaitingToUpload,FileCounter)
			Newpath = os.path.abspath(Metadata(WriteToMeta_list,size_list,CombinedFileName))
			NewFileName.append(Newpath)
			total_size=0
			combine_list=[]
			WriteToMeta_list=[]
			size_list=[]
	#break the for loop ,combined remaining files
	FileCounter = FileCounter +1
	CombinedFileName = Combine_files(combine_list,rootDir,WaitingToUpload,FileCounter)
	Newpath = os.path.abspath(Metadata(WriteToMeta_list,size_list,CombinedFileName))
	NewFileName.append(Newpath)	

	BigFileMetadata(BigFileList,WaitingToUpload)
