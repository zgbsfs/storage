import os
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

Percentage = 0.05
MB = 1024*1024.0
def Compress_list(list_of_file) :
#       print WaitingToUpload  /Users/ytlin/GitHub/storage/1g/
#       print path             every dir
#       print list_of_file     list...... WITH FULL PATH
#       print sourceDir          /Users/ytlin/Desktop/poty2006/
#       print inputforname       (license)(style)
#       print S3KeyName
	t1 = time.time()
        b = 'sample_file_removenow'
        tar=tarfile.open(b,'w:gz')
	orisize =0
        for f in list_of_file:
		orisize += os.path.getsize(f)
                tar.add(f)
        tar.close()
	t2 = time.time()
	dur = t2 -t1
	new = os.path.getsize(b)
	print "  compress    " + str(new)
	print "-------------"
	print "  ori         " + str(orisize)
	print '=  '+str(float(new)/float(orisize))

	print "duration     "  +str(dur)
	throughput = orisize/float(dur)
	

        return float(new)/float(orisize),throughput/(1024*1024.)
def Sampling(filepath,S3KeyName,CombineThreshold,True ,avgFilesize):
#	print filepath  /Users/ytlin/Desktop/poty2006/
#	print a /1g/
#	print avgFilesize
#	print CombineThreshold
	maxThroughput =0
	randomfilelist=[]
	returnarr=[]
	dic={}
	for ( sourceDir, dirname,filename) in os.walk(filepath):
		print sourceDir
#		print dirname
#		print filename
		dic['path'] = sourceDir
		randomtime =int(Percentage*len(filename))

		if len(filename) == 0:
			continue
		if randomtime < 1:
			randomtime=1
		print str(len(filename))+"   files choose  "+ str(randomtime)		
		for i in range(randomtime):
			a = random.choice(filename)
			filename.remove(a)
			randomfilelist.append(os.path.join(sourceDir, a))

		rate,throughput=Compress_list(randomfilelist)
		dic['rate'] = rate
		dic['throughput'] = throughput
		returnarr.append(dic.copy())
		if maxThroughput <throughput:
			maxThroughput = throughput


	return returnarr ,maxThroughput
