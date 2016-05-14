from shutil import copyfileobj
import magic
import gzip
import os
import os.path
import tarfile
import sys
import pickle
import time
import subprocess
import  random
import operator
import binpack

MAX_SIZE = 5 * 1000 * 1000
#Threshold = 30
MB = 1024*1024
Formatlist = ['compress','video','audio','jpeg','mpeg','gzip','zip','gif','png']

def Combine_files(combine_list,sourceDir,WaitingToUpload,FileCounter):
	CombinedFileName = sourceDir.replace(sourceDir,WaitingToUpload)+str(FileCounter)+"-data"
	with open(CombinedFileName, 'wb') as outfile:
		for fname in combine_list:
			with open(fname,'r') as infile:
				outfile.write(infile.read())
	return CombinedFileName

def file_to_bin(combine_list,sourceDir,WaitingToUpload,FileCounter):
        CombinedFileName = sourceDir.replace(sourceDir,WaitingToUpload)+"bin"+str(FileCounter)
        with open(CombinedFileName, 'wb') as outfile:
                for fname in combine_list:
                        with open(fname,'r') as infile:
                                outfile.write(infile.read())
#                               os.remove(fname)
        return CombinedFileName 
def Combine_file(combine_list,sourceDir,WaitingToUpload,FileCounter,inputforname):
	if inputforname=='':
		inputforname='UnderRoot'
        CombinedFileName = sourceDir.replace(sourceDir,WaitingToUpload)+inputforname.replace('/','_')+str(FileCounter)+"_data"
        with open(CombinedFileName, 'wb') as outfile:
                for fname in combine_list:
                        with open(fname,'r') as infile:
                                outfile.write(infile.read())
#                               os.remove(fname)
        return CombinedFileName

def Metadata(MetaFileInfo,MetaSizeInfo,NewFileName):
	MetaName = NewFileName+".meta"
	sizeName = NewFileName+".size"
	with open(MetaName, 'wb') as f:
		pickle.dump(MetaFileInfo, f)
	with open(sizeName, 'wb') as f:
		pickle.dump(MetaSizeInfo, f)
	return os.path.abspath(MetaName),os.path.abspath(sizeName)

def Compress_Meta_list(list_of_file,Dirpath,S3KeyName) :
#       print WaitingToUpload  /Users/ytlin/GitHub/storage/1g/
#       print path             every dir
#       print list_of_file     list...... WITH FULL PATH
#       print rootDir          /Users/ytlin/Desktop/poty2006/
        #b = sourceDir.replace(sourceDir,WaitingToUpload)+inputforname.replace('/','_')+'files.tar.gz'
	b = Dirpath+"Meta.tar.gz"
        tar=tarfile.open(b,'w:gz')
        for f in list_of_file:
#		name in tar file is s3keyname+dataname  ex 1g/bin1.meta
		tar.add(f,arcname=f.replace(Dirpath,S3KeyName))
		
        tar.close()
        new = os.path.getsize(b)
	
        return b,new
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
                        if ".size" in f:

				print "sizelist         "+str(f)
				metaindir = os.path.join(sourceDir, f)
                                MetadataInDir.append(metaindir)
			elif ".meta" in f:
				print "metalist         "+str(f)
                                metaindir = os.path.join(sourceDir, f)
                                MetadataInDir.append(metaindir)
				
			#	totalsize += os.path.getsize(metaindir)
			#	size_list.append(totalsize)
				
	return MetadataInDir #, size_list
def Compress_list(list_of_file,sourceDir,WaitingToUpload,inputforname,S3KeyName) :
#	print WaitingToUpload  /Users/ytlin/GitHub/storage/1g/
#	print path             every dir
#	print list_of_file     list...... WITH FULL PATH
#	print sourceDir          /Users/ytlin/Desktop/poty2006/
#	print inputforname       (license)(style)
#	print S3KeyName
	if inputforname=='':
		inputforname='UnderRoot'
	b = sourceDir.replace(sourceDir,WaitingToUpload)+inputforname.replace('/','_')+'-files.tar.gz'
	'''    back up
	compression file bigger than expected compression size
	filearr=[]
	sizearr =[]
	readsize=0
	part=0
	filearr.append(b)
	'''
	tar=tarfile.open(b,'w:gz')

	for f in list_of_file:
		tar.add(f,arcname=f.replace(sourceDir,S3KeyName))
		'''   backup
		readsize +=os.path.getsize(f)
		if Totalsize > readsize:	
			tar.add(f,arcname=f.replace(sourceDir,S3KeyName))
		else:# many file situation 
			part+=1
			tar.close()
			newname = b+str(part)
			filearr.append(newname)
			tar=tarfile.open(newname,'w:gz')
			tar.add(f,arcname=f.replace(sourceDir,S3KeyName))
			readsize = os.path.getsize(f)
		'''	
	tar.close()
	'''   backup
	if part >0:#many file situation 
		for f in filearr:
			sizearr.append(os.path.getsize(f))	
		return filearr,sizearr
	else:
		new = os.path.getsize(b)
		return b,new
	'''
	new = os.path.getsize(b)
	return b,new
def Compress_all_files(Dirpath,S3KeyName):
	CompressfilesInDir = []
	totalsize = 0
        meta_list =[]
	print S3KeyName
	name = Dirpath+"Meta.tar.gz"
	CompressfilesInDir.append(name)
	for (sourceDir, dirname, filename) in os.walk(Dirpath):
                for f in filename:
			fileindir = os.path.join(sourceDir, f)
			if ".meta"  in f :
				print f
				meta_list.append(fileindir)
                        elif ".size" in f:
				print f
				meta_list.append(fileindir)
			else:
                                CompressedFile = fileindir+".gz"
                                CompressfilesInDir.append(CompressedFile)
                                with open(fileindir, 'rb') as input:
                                    with gzip.open(CompressedFile, 'wb') as output:
                                            copyfileobj(input, output)

        #                       totalsize += os.path.getsize(CompressedFile)
        #                       size_list.append(totalsize)
	tar=tarfile.open(name,'w:gz')
	for fileindir in meta_list:
		tar.add(fileindir)
	tar.close()
	return CompressfilesInDir #,size_list

def CompressBigFile(BigFilePath,S3KeyName,rootDir,WaitingToUpload,InDir):
	#Formatlist = ['compress','video','audio','jpeg','mpeg','gzip','zip']
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

def Compression(rootDir,S3KeyName,Threshold,compression,listofsampling,expectSpeed,max_compression_throughput):
	print expectSpeed
	print max_compression_throughput
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
	localtime = time.asctime( time.localtime(time.time()))
	sampling=[]
	UploadFileList=[]
	'''
	print rootDir	
	p1 = subprocess.Popen(['find' ,str(rootDir),'-type','f'], stdout=subprocess.PIPE)
#	print proc.stdout.read()
	p2 = subprocess.Popen(["wc","-l"], stdin=p1.stdout,stdout=subprocess.PIPE)
	p1.stdout.close()
	output,err = p2.communicate()
	print int(output)

	return
	'''
	print "Local current time :", localtime
#	sample_threshold = int(sampling_percentage*Threshold*1024/avg_file_size)
#	print "how many file need to ?? "  +str(sample_threshold)
	sampling_count = 0
	samplesize =0
	'''
	print os.listdir(rootDir)
	for i in range(5):

		print random.choice(os.listdir(rootDir))
	'''
	metalist=[]
	item={}
	itemlist =[]
	for dic in listofsampling:
		mypath = dic['path']
		rate = dic['rate']
		inputforname = mypath.replace(rootDir,'')
		#compression rate poorly,files to be combined
		if (1-rate)*max_compression_throughput < expectSpeed:
			FileCounter=0
			for f in os.listdir(mypath) :
			    if not os.path.isfile(os.path.join(mypath, f)):
				continue
			    sourcepath =  os.path.join(mypath, f)
			    if  Combine_size+os.path.getsize(sourcepath) < 1024 * MB:
				    if os.path.getsize(sourcepath) > Threshold * MB:
					localtime = time.asctime( time.localtime(time.time()) )
					print "Local current time :", localtime
					#big file list					
					Uploadsize += os.path.getsize(sourcepath)
					Newpath , Metapath = CompressBigFile(sourcepath,S3KeyName,rootDir,WaitingToUpload,True)
					UploadBigFileList.append(Newpath)
					BigFileList.append(Metapath)
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

				CombinedFileName = Combine_file(combine_list,rootDir,WaitingToUpload,FileCounter,inputforname)
				metapath,sizepath = Metadata(WriteToMeta_list,size_list,CombinedFileName)
				item[CombinedFileName]  = Combine_size
				itemlist.append(CombinedFileName)
				NewFileName.append(metapath)
				NewFileName.append(sizepath)
				Uploadsize += Combine_size
				Combine_size=0
				combine_list=[]
				WriteToMeta_list=[]
				size_list=[]
			if Combine_size >0:
				Uploadsize += Combine_size
				FileCounter = FileCounter +1
				CombinedFileName = Combine_file(combine_list,rootDir,WaitingToUpload,FileCounter,inputforname)
				metapath,sizepath = Metadata(WriteToMeta_list,size_list,CombinedFileName)
				item[CombinedFileName]  = Combine_size
				itemlist.append(CombinedFileName)
                                Combine_size=0
                                combine_list=[]
                                WriteToMeta_list=[]
                                size_list=[]
				NewFileName.append(metapath)
				NewFileName.append(sizepath)
		#compression rate better,compressed the file in the directory
		else:
			onlyfiles = [os.path.join(mypath, f) for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath, f))]
			metalist.append(mypath)
			return_path ,return_size= Compress_list(onlyfiles,rootDir,WaitingToUpload,inputforname,S3KeyName)
			if return_size > Threshold * MB:				
				UploadBigFileList.append(return_path)
			else:
				item[return_path]  = return_size
	return itemlist,10000000
	print "start bin pack"
	sorted_x = sorted(item.items(), key=operator.itemgetter(1), reverse=True)
	bins =binpack.packAndShow(sorted_x,Threshold*MB)
	binnum=0
	UploadFileList+= UploadBigFileList
	for bin in bins:
		WriteToMeta_list=[]
		binnum+=1
		file_in_bin , size_in_bin =  bin.getitempath()
		uploadbins = file_to_bin(file_in_bin,rootDir,WaitingToUpload,binnum)
		UploadFileList.append(uploadbins)	
		for item in file_in_bin:
			WriteToMeta_list.append(item.replace(os.getcwd(),''))
		metapath,sizepath = Metadata(WriteToMeta_list,size_in_bin,uploadbins)
		NewFileName.append(metapath)
		NewFileName.append(sizepath)
		CompressedMeta, the_filesize = Compress_Meta_list(NewFileName,WaitingToUpload,S3KeyName)
	UploadFileList.append(CompressedMeta)
	print UploadFileList
	print UploadBigFileList
	totalupsize=0
	for i in UploadFileList+UploadBigFileList:
		totalupsize+=os.path.getsize(i)

	return UploadFileList+UploadBigFileList ,totalupsize
	'''
	for ( sourceDir, dirname, filename) in os.walk(rootDir):
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
		 	metapath,sizepath = Metadata(WriteToMeta_list,size_list,CombinedFileName)
			NewFileName.append(metapath)
			NewFileName.append(sizepath)
			Uploadsize += Combine_size
			Combine_size=0
			combine_list=[]
			WriteToMeta_list=[]
			size_list=[]
	#break the for loop ,combined remaining files
	if Combine_size >0:
		Uploadsize += Combine_size
		FileCounter = FileCounter +1
		#print "other + percentage  " + str(Y/(Y+N))
		CombinedFileName = Combine_files(combine_list,rootDir,WaitingToUpload,FileCounter)
		metapath,sizepath = Metadata(WriteToMeta_list,size_list,CombinedFileName)
		NewFileName.append(metapath)
		NewFileName.append(sizepath)

	if BigFileList: 
		BigFileMetadata(BigFileList,WaitingToUpload)
	b=0
	Metadatalist     = Metadata_list(WaitingToUpload)
#	with open('testaaaaaaaa', 'wb') as f:
#		pickle.dump(sampling, f)
	
	if not compression:
		for f in NewFileName+UploadBigFileList+Metadatalist:
			b+=os.path.getsize(f)
		print "== FAlse"
		return NewFileName+UploadBigFileList,Metadatalist,b
	else:
		Compressfilelist = Compress_all_files(WaitingToUpload,S3KeyName)


		for f in UploadBigFileList+Compressfilelist:
			b+=os.path.getsize(f)
		print "== TRUe"
		return UploadBigFileList+Compressfilelist ,Metadatalist,b
	'''
	'''
	Combine_files(Compressfilelist,rootDir,WaitingToUpload,"upload")
	print combine_size_list
	print '\n'
	Metafilelist ,combine_meta_size_list = Metadata_list(WaitingToUpload)
	Combine_files(Metafilelist,rootDir,WaitingToUpload,"Meta")
	print combine_meta_size_list
	print '\n'
	'''
