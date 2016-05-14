#!/usr/bin/env python
import copy
import argparse
import os
import errno
from cStringIO import StringIO
import logging
from math import ceil
from multiprocessing import Process, Value, Array,Lock,current_process,active_children,Manager ,Pool
#import multiprocessing.pool
import time
import urlparse
from timeout import timeout
import boto
from boto.s3.connection import S3Connection
from random  import randint
import random
import sys, traceback
import signal
import compress
from functools import partial
import subprocess
import shutil
import sample
import getfile
from threading import Thread,Lock,currentThread
from traffic import initargs
import numpy
import psutil
from traffic import bytes2human
from livestats import livestats

parser = argparse.ArgumentParser(description="Transfer large files to S3",
        prog="s3-mp-upload")
parser.add_argument("filepath",  help="The file to transfer")
parser.add_argument("dest", help="The S3 destination object")
parser.add_argument("-n", "--num-processes", help="Number of processors to use",
        type=int)
parser.add_argument("-f", "--force", help="Overwrite an existing S3 key",
        action="store_true")
parser.add_argument("-s", "--split", help="Split size, in Mb", type=int)
parser.add_argument("-Thres", "--Threshold", help="compression size", type=int)
parser.add_argument("-S", "--simulate", help="Enable simulation with the time per error ",type=int ,default=0)
parser.add_argument("-get",help="download the single file from s3 path ", default=False,  action="store_true")
parser.add_argument("--Retransmit", help="Enable Retransmition simulation " ,default=False,  action="store_true")

logger = logging.getLogger("s3-mp-upload")

#class NoDaemonProcess(multiprocessing.Process):
# make 'daemon' attribute always return False
#    def _get_daemon(self):
#    	return False
#    def _set_daemon(self, value):
#	pass
#    daemon = property(_get_daemon, _set_daemon)

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
#class NoDaemonProcessPool(multiprocessing.pool.Pool):
#	Process = NoDaemonProcess

sp=10
uploadtime=[]
def handler(signum, frame):
	raise UserWarning("failure occur!!")
@timeout(sp, os.strerror(errno.ETIMEDOUT))
def do_part_upload(args,current_tries=1):
    # Multiprocessing args lameness
    bucket_name, mpu_id, fname, i, start, size,  current_tries = args
    #print args
    logger.debug("do_part_upload got args: %s" % (args,))
    # Connect to S3, get the MultiPartUpload
    s3 = S3Connection()
    bucket = s3.get_bucket(bucket_name)
    mpu = None
    #print "which process  " +str(i) 
    for mp in bucket.list_multipart_uploads():
        if mp.id == mpu_id:
            mpu = mp
            break
    if mpu is None:
        raise Exception("Could not find MultiPartUpload %s" % mpu_id)
    # Read the chunk from the file
    fp = open(fname, 'rb')
    fp.seek(start)
    data = fp.read(size)
    fp.close()
    if not data:
        raise Exception("Unexpectedly tried to read an empty chunk")
    try:
        # Do the upload
#	print "which part "+ str (i)
        t1 = time.time()
	mpu.upload_part_from_file(StringIO(data), i)
        t2 = time.time() - t1
        s = len(data)/1024./1024.
	print start
	logger.error("Uploaded part %s (%0.2fM) in %0.3f s at %0.2f MBps" % (i, s, t2, s/t2))
	return t2
    except KeyboardInterrupt:
	print "detect in here"
    except IOError:
	print "IO   in here"
    except Exception, err:
	print "time error     " + str(mpu) + "  part  " +str(i)
#	current_tries = current_tries+1
	#traceback.print_exc()
	logger.error(err)
#        logger.info("Retry request %d " % (current_tries, max_tries))
	#if current_tries <max_tries:
	#        do_part_upload(args,current_tries)
	#else:
	#	return
	global sp
	return sp*2


def Thread_Upload(Total_Threadnum,Threadnum,uploadFileNames,FileList,status_list,lock,que,filepath, dest, num_processes=2, split=50, force=False, simulate=0,Threshold=0,get=False,Retransmit=False):
    if status_list[FileList.index(uploadFileNames)] =='finish':
		print str(uploadFileNames)+"  you shoud not here"
    #this is work  print currentThread().getName()
    print "I am " +str(uploadFileNames) +" Retrans = " +str(Retransmit)+" proc= "+str(num_processes) +" status = " +str(status_list[FileList.index(uploadFileNames)])
    # Check that dest is a valid S3 url
    if  Retransmit=='init':
	    lock.acquire()
	    status_list[FileList.index(uploadFileNames)] = str('upload')
	    lock.release()
	    print "inthread "  + status_list[FileList.index(uploadFileNames)]
    elif Retransmit:
	    if type(status_list[FileList.index(uploadFileNames)])==int:
		    lock.acquire()
		    status_list[FileList.index(uploadFileNames)] = str('upload')
		    lock.release()
		    print "retran but no mpu" 

    split_rs = urlparse.urlsplit(dest)
    filepath = uploadFileNames
    src = open(filepath, "rw+")

    filepath = uploadFileNames.replace(os.getcwd(),"")

    global sp
    sp=int(get)

    s3 = S3Connection()

    key = bucket.get_key(filepath)
    # See if we're overwriting an existing key
    if key is not None:
        if not force:
            raise ValueError("'%s' already exists. Specify -f to overwrite it" % dest)
    # Determine the splits

    part_size = max(5*1024*1024, 1024*1024*split)
    src.seek(0,2)
    size = src.tell()
    num_parts = int(ceil(size / part_size))

    # If file is less than 5M, just upload it directly
    if size < 1024*1024*1024:
	print src.name +'  single use ' +str (num_processes)

	src.seek(0)
	t1 = time.time()
	k = boto.s3.key.Key(bucket,filepath)
	logger.error(str(src.name) +" start ")

	k.set_contents_from_file(src)
	logger.error(str(src.name) +" stop " )
	t2 = time.time() - t1
	s = size/1024./1024.
	'''
	hdlr = logging.FileHandler("bigmultipart.log")
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	hdlr.setFormatter(formatter)
	#logger.addHandler(hdlr)
	#logger.info("part size = %d ,concurrency = %d ,Finished uploading %0.3f M in %0.3f s (%0.3f MBps)" % (split ,num_processes, s, t2, s/t2))
	'''
	print "index in proc = "+str(FileList.index(uploadFileNames))
	lock.acquire()
	status_list[FileList.index(uploadFileNames)]='finish'
	print src.name +" finish  "+str (status_list)
	critical_threadnum(Total_Threadnum,Threadnum,num_processes)
	print "finish " + uploadFileNames +" add back now is   " + str(Threadnum.value)
	lock.release()
	#os.remove(src.name)
	return
    if Retransmit=='init':
        # Create the multi-part upload object
        mpu = bucket.initiate_multipart_upload(filepath)
	lock.acquire()
	status_list[FileList.index(uploadFileNames)]=mpu
	lock.release()
    elif Retransmit  :
	if status_list[FileList.index(uploadFileNames)] =='upload':
		mpu = bucket.initiate_multipart_upload(filepath)
		lock.acquire()
		status_list[FileList.index(uploadFileNames)]=mpu
		lock.release()
	else:
		mpu = status_list[FileList.index(uploadFileNames)]

    else:
	print str(filepath) +"   is " +str(Retransmit)
	print "this is " +str( type(status_list[FileList.index(uploadFileNames)]))
	mpu = status_list[FileList.index(uploadFileNames)]
	'''
	for mp in bucket.list_multipart_uploads():
            if mp.id == status_list[FileList.index(uploadFileNames)].id:
        	    mpu = mp
	            break
	'''
    # Generate arguments for invocations of do_part_upload
    def gen_args(x, fold_last, upload_list):
        for i in upload_list:
            part_start = part_size*(i-1)
	    if i == num_parts and fold_last is True:
	    #	print "fold_last" +src.name
                yield (bucket.name, mpu.id, src.name, i, part_start, part_size+5*1024*1024,  0)
	    else:
                yield (bucket.name, mpu.id, src.name, i, part_start, part_size,  0)
    # If the last part is less than 5M, just fold it into the previous part
    fold_last = ((size % part_size) < split*1024*1024)
    upload_list=[]
    for i in range(1,num_parts+1):   
	    upload_list.append(i)
    # Do the thing
    t1 = time.time()

    def master_progress(mpu,num_processes,bucket,upload_list):
	    x=0
	    '''
	    x= len(mpu.get_all_parts())
	    for i in mpu.get_all_parts():
		a=int(str(i).split(" ")[1].split(">")[0])
		if a in upload_list:
			upload_list.remove(a)
	    '''
	    print "proc = ?? "  + str(num_processes)
	    while True:
		try:
			if x!=num_parts:
				print " num parts = "+str(num_parts)
	#			logger.error(str(src.name) +" start " )
				pool = Pool(processes=num_processes)
				value = pool.map_async(do_part_upload, gen_args(x,fold_last,upload_list)).get(99999999)
#			print "dadadada " + str(value)
			que.put(value)
			src.close()
                        mpu.complete_upload()
			logger.error(str(src.name) +" stop " )
			#proc = subprocess.Popen('date', stdout=subprocess.PIPE)
			#print stdout
			print "mpu.complete src name " +src.name
			#os.remove(src.name)
			#print "index in proc = "+str(FileList.index(uploadFileNames))
			lock.acquire()
			status_list[FileList.index(uploadFileNames)]='finish'
			print src.name +" finish  "+str (status_list)
			critical_threadnum(Total_Threadnum,Threadnum,num_processes)
			print uploadFileNames +" add back now is   " + str(Threadnum.value)
			lock.release()
			src.close()
			return value
#			pool.terminate()
                        break
		except KeyboardInterrupt:
			logger.warn("Received KeyboardInterrupt, canceling upload")
			pool.terminate()
			mpu.cancel_upload()
			print "keyboarddddddddddddddddddddddddddddddd"
			break
		except IOError:
			break
		except Exception, err:
			logger.error("Encountered an error, canceling upload aaaaaaaaaaaa")
			print src.name
			logger.error(str(src.name)+str(err))

    t1 = time.time()
    master_progress(mpu,num_processes,bucket,upload_list)

def getBandwidth(bucket):
	print "start speedtest"
	size = 1024*1024*25
	chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
	data = chars * (int(round(int(size) / 36.0)))
	testuploaddata = ('content1=%s' % data[0:int(size) - 9]).encode()
	del data
	@timeout(5, os.strerror(errno.ETIMEDOUT))
	def uploadtest(testuploaddata,bucket):
	    try:
		t1 = time.time()
		print "start s3 uploadtest "
		tot_before = psutil.net_io_counters()
		k = boto.s3.key.Key(bucket,'uploadtest')
		k.set_contents_from_string(testuploaddata)
		t2 =time.time() -t1
		sitethroughput = 25./t2
		return sitethroughput
	    except Exception, err:
		print err
		print "exce"
		tot_after = psutil.net_io_counters()
		diff_sent = tot_after.bytes_sent-tot_before.bytes_sent
		print 'sent : '+ str(bytes2human(diff_sent)) +" in %d second ,speed = " % (10)+str(bytes2human(diff_sent/10))
		return diff_sent/1000/1000/10.

	sitethroughput = uploadtest(testuploaddata,bucket)	
	proc = subprocess.Popen(['speedtest-cli','--bytes'], stdout=subprocess.PIPE)
	strlist = proc.stdout.read().split(" ")
	print "bandwidth = "+ strlist[len(strlist)-2]
	return  float(strlist[len(strlist)-2]),sitethroughput

def critical_threadnum(Total_Threadnum,Threadnum,addback):
	NewValue = Total_Threadnum.value
        if Threadnum.value +addback > NewValue:
		Threadnum.value = NewValue
        else:
		Threadnum.value += addback

def FIFO(arg_dict,uploadFileNames,bandwidth,input_threadnum):
    #return 
    manager = Manager()
    status_list = manager.list(range(len(uploadFileNames)))
    q = manager.Queue()
#    q.put([432.1])
#    status_backup = manager.list(range(len(uploadFileNames)))
    Threadnum = Value('i', input_threadnum)
    Total_Threadnum = Value('i', input_threadnum)
    print "wwwww   +" + str(status_list)
    lock = Lock()
    toThread = arg_dict.copy()
    change_thread  = Process(name='ChangeResource',target=NewResourceSize,args=(Total_Threadnum,Threadnum,lock,bandwidth,status_list))
    change_thread.daemon = True
	
    change_thread.start() 
    dict_list=[]
    Failure = 0
    for i in range(len(uploadFileNames)):
	dict_list.append("new")
    print "how much file " + str(len(uploadFileNames))
    in_the_while_iter =0
    single = toThread['get']
    split_size = toThread['split']
    time_list=[]
    noreturnpart=True
    while True:
	    try:
		    if in_the_while_iter>len(uploadFileNames)-1:
			    in_the_while_iter=0
		    if status_list.count('finish') == len(uploadFileNames):
			    logger.error(dict_list)			  
			    return Failure
		    if Failure>0:
			    print str(status_list)
		    if Threadnum.value >0 and status_list[in_the_while_iter]==in_the_while_iter:
			    # or str(type(status_list[in_the_while_iter]))=="<class 'boto.s3.multipart.MultiPartUpload'>"):
#			    print str(Threadnum.value)  +"  num "+str(in_the_while_iter)+ " int  " + str(status_list[in_the_while_iter]) + " wha   " + str(type(status_list[in_the_while_iter]))+ " type  " 
#			    dict_list[i]['Retransmit']
#			    if dict_list[i]['Retransmit']==True:
#				    dict_list[i]['Retransmit']==False
			    if dict_list[in_the_while_iter]=='new':
				    toThread['filepath'] ,toThread['num_processes'] = FIFOargs(uploadFileNames,in_the_while_iter,arg_dict['split'],arg_dict['num_processes'])
				    toThread['Retransmit']='init'
				    while not q.empty():
					    time_list+=q.get()
					    print "get data" + str( time_list)
				    if time_list:
					    percentile_data = livestats.LiveStats([0.9])
					    time_iter = iter(time_list)
					    for x in xrange(len(time_list)):
						percentile_data.add(time_iter.next())
					    toThread['get'] = percentile_data.quantiles().pop()[1]*2
					    noreturnpart=False
					    time_list=[]
				    if noreturnpart:
					    toThread['get'] = max(split_size/single,split_size/(bandwidth/Total_Threadnum.value))*3
				    dict_list[in_the_while_iter] = toThread.copy()
			    elif dict_list[in_the_while_iter]['Retransmit']=="uploading":
                                    in_the_while_iter+=1                           
                                    continue
			    else:	
  				    dada=0
		#	    print str(uploadFileNames[in_the_while_iter])+"   now is  here " +str(dict_list[in_the_while_iter]) 
			    if  Threadnum.value < toThread['num_processes'] :
				if dict_list[in_the_while_iter]['Retransmit']=='init':
#				    print " i is  ??= "+str(i) +"  " +str(uploadFileNames[i])+ " here   "+ str(Threadnum.value)+ "  No here  " +str(dict_list[i])

#				    time.sleep(1)
				    dict_list[in_the_while_iter]='new'
	#			    print " I =  "+str(in_the_while_iter) + "  ???  "+ str(dict_list[in_the_while_iter])
				    in_the_while_iter+=1
				elif dict_list[in_the_while_iter]['Retransmit']:
				    in_the_while_iter+=1
				    print " TUREEEEE"
			        continue
			    
			    func = partial(Thread_Upload,**dict_list[in_the_while_iter])
			    a = Process(name=uploadFileNames[in_the_while_iter],target=func,args=(Total_Threadnum,Threadnum,uploadFileNames[in_the_while_iter],uploadFileNames,status_list,lock,q))
			    a.daemon =False
			    a.start()
		            
			    dict_list[in_the_while_iter]['Retransmit']="uploading"
			    Threadnum.value -=  dict_list[in_the_while_iter]['num_processes']
			    print str(uploadFileNames[in_the_while_iter])+"   please here " +str(dict_list[in_the_while_iter])
#			    time.sleep(1) 
			    in_the_while_iter+=1
		    else:
			    in_the_while_iter+=1
#		    print status_list
		       
	    except IndexError:
		continue
	    except KeyboardInterrupt,Exception:
		traceback.print_exc()
		print "FIFO ^c"

		break
	    except UserWarning:
		Failure +=1
		if arg_dict['split'] >6:
			arg_dict['split'] -=1
		print active_children()
		print dict_list
#		for index,content in enumerate(status_list,1):#range(len(uploadFileNames)):
#                       print str(type(status_list[index]) )+ " "+str(index)+"  =  " +str(status_list[index])
#			print  (index,content)
		for task_process in  active_children():
			#print str(task_process) +"   what type " + str(type(task_process))
			if str(task_process)=="<Process(SyncManager-1, started)>":
				print "anyone?  " +str(task_process)
				continue
			else:
				task_process.terminate()
		time.sleep(4)
	
		for index,content in enumerate(status_list,Failure):#range(len(uploadFileNames)):
			print str(index) +str(content)
#			status_list[index-1]=content
                        if content==str('upload'):
				print "yes"
#				print str(type(status_list[i]) )+ "   = " +str(status_list[i])
                                content=int(index)
			print str(index) +str(content)
				
		print "after  Dont plerase" + str(active_children())
		for item in dict_list:
			if item != 'new':
				item['Retransmit'] =True
		in_the_while_iter=0
		lock.acquire()
		Threadnum.value=10
		lock.release()
		Process(name='ChangeResource',target=NewResourceSize,args=(Total_Threadnum,Threadnum,lock,bandwidth,status_list)).daemon =True
		Process(name='ChangeResource',target=NewResourceSize,args=(Total_Threadnum,Threadnum,lock,bandwidth,status_list)).start()
		#signal.alarm(int(random.expovariate(1.0/arg_dict['simulate'])))
		signal.alarm(15)
		pass

def FIFOargs(uploadFileNames,now,split,inputProcessNum):
	sizeinMB = os.path.getsize(uploadFileNames[now])/1024./1024.
	num_process = int(ceil(sizeinMB/split))
	if inputProcessNum < num_process:
		num_process = inputProcessNum
#	print uploadFileNames[now],num_process
	return uploadFileNames[now],num_process
def NewResourceSize(Total_Threadnum,Threadnum,lock,bandwidth,status_list):

	last_Threadnum = copy.copy(Total_Threadnum.value)

	#print copy.copy(Total_Threadnum.value)
	last_throughput = bandwidth
	counter = 0
	time.sleep(2)
	history_throughput = []
	STD = 50
	print "start to computing throught"
	while True:
		try:
			if status_list.count('finish') == len(uploadFileNames):
				
				break
			if STD>0.5:
				Unstable = True
			else:
				print "stable throughput =  "+str (numpy.median(history_throughput))
				Unstable = False
			if Unstable:
				now_throughput = initargs(5,)
				now_throughput = now_throughput/1024./1024.
				if len(history_throughput)>3 :
#					print "WTFFFF  " +str(numpy.std(history_throughput))
					STD = float(numpy.std(history_throughput))

					print "standard XXXXX " + str(STD)
					history_throughput.pop()
					history_throughput.insert(0,now_throughput)
				else:
					history_throughput.insert(0,now_throughput)
				print history_throughput
				if now_throughput < (last_throughput/2) or counter>2:
					print str(last_throughput) + " is higher than "+str(now_throughput)+" ,increase resource "
					
					Total_Threadnum.value += 5
					last_throughput = now_throughput
					counter = 0
				else:
					
					counter+=1
				if Total_Threadnum.value > last_Threadnum :
					proc = subprocess.Popen(['date'], stdout=subprocess.PIPE)
					strlist = proc.stdout.read()
					logger.error("resource increse  " + str(Total_Threadnum.value-last_Threadnum))
					lock.acquire()
					Threadnum.value += Total_Threadnum.value-last_Threadnum
					lock.release()
					print "resource increase "+str(Total_Threadnum.value -last_Threadnum)+" now is " +str(Threadnum.value) 
					last_Threadnum = copy.copy(Total_Threadnum.value)
					print "Total_Threadnum Now is %d,Can be use is %d"% (Total_Threadnum.value,Threadnum.value) 	
			else:
				initargs(5,)
		except KeyboardInterrupt:
			print "NewResource ^c"
			break

if __name__ == "__main__":
    #logging.basicConfig(level=logging.INFO)

    args = parser.parse_args()
    arg_dict = vars(args)
    if arg_dict['get']:
	    getfile.GetTheFile(arg_dict['filepath'],arg_dict['dest'])
    else:
	    split_rs = urlparse.urlsplit(arg_dict['dest'])
	    if split_rs.scheme != "s3":    
		raise ValueError("is not an S3 url")
	    s3 = S3Connection()
	    tprofile = time.time()
	    bucket = s3.get_bucket(split_rs.netloc)
	    if bucket == None:
            	raise ValueError("'%s' is not a valid bucket" % split_rs.netloc)

	    hdlr = logging.FileHandler("eu-westbigmultipart.log")
	    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	    hdlr.setFormatter(formatter)
	    logger.addHandler(hdlr)
	    logger.setLevel(logging.ERROR)
	
	    bandwidth,singlethroughput = getBandwidth(bucket)	  
	    print "single thr %f " % singlethroughput
	    #print "bandwidth = "+ str(bandwidth)
#	    bandwidth = 10
	    filepath = arg_dict['filepath']
	    # it work for du -sh *,get the size
	    print split_rs
	    print filepath
	    proc = subprocess.Popen(['du','-sk',filepath], stdout=subprocess.PIPE)
	    strlist = proc.stdout.read().split("\t")
	    print 'origin size is ' +str(strlist[0])
	
	    Uploadsize = float(strlist[0])

	    uploadFileNames = []
	    Upload_dir_Name = os.getcwd()+split_rs.path
	    tp = time.time()-tprofile
	    t1 = time.time()
	    if not arg_dict['Threshold']:
		    arg_dict['Threshold'] =max(15,int(bandwidth*5/2))
	    if not arg_dict['split']:
		    arg_dict['split'] = max(5,int (bandwidth*3/10))
	    if not arg_dict['num_processes']:
		    arg_dict['num_processes'] = max(5,int(bandwidth/2/singlethroughput))

	    Threadnum =  max(10,int(bandwidth/singlethroughput))

	    arg_dict['get'] = singlethroughput
	    
# max((arg_dict['split']/singlethrougGhput),arg_dict['split']/(bandwidth/Threadnum))*2
	    print arg_dict
	    if os.path.isfile(filepath):
		Upload_dir_Name ,nocompression= compress.CompressBigFile(filepath,split_rs.path,filepath,Upload_dir_Name,False)
		uploadFileNames.append(Upload_dir_Name)
		afterCompressSize = 0
		Uploadsize = os.path.getsize(Upload_dir_Name)/1024.
		tcompression=time.time()
	    else:
		# find ./ -type f |wc -l
		tsam=time.time()
                p1 = subprocess.Popen(['find' ,str(filepath),'-type','f'], stdout=subprocess.PIPE)
                p2 = subprocess.Popen(["wc","-l"], stdin=p1.stdout,stdout=subprocess.PIPE)  
                p1.stdout.close()
                output,err = p2.communicate()
		number_of_file = int(output)
                print ' number of files' + str(output)
		print ' start sampling'
		listofsampling,max_compression_throughput = sample.Sampling(filepath,split_rs.path,arg_dict['Threshold'],True ,Uploadsize/number_of_file)
		### split_rs.path = /1g/
		tsample =time.time()-tsam
		tcompression=time.time()
	 	GZfile,afterCompressSize = compress.Compression(filepath,split_rs.path,arg_dict['Threshold'],True ,listofsampling,bandwidth,max_compression_throughput)
		uploadFileNames = GZfile
	    t3 = time.time() - tcompression

	    print Threadnum
	    print uploadFileNames 
	    if Threadnum ==0:
		    pass
	    else:
		    Howmany = FIFO(arg_dict,uploadFileNames,bandwidth,Threadnum)

	    t2 = time.time() - tprofile
	    print "total time= "+str(t2)
	    print "fail %d times" % Howmany
	    print 'upload time array  ' +str(uploadtime)
	    Uploadsize = Uploadsize/1024.
	    afterCompressSize = afterCompressSize/1024./1024.
	    rate = (Uploadsize -afterCompressSize) /t3
	    if os.path.isfile(filepath) and nocompression:
		 logger.error("Nocompression  File %s part size = %d ,concurrency = %d ,upload (ori)%0.2fM in %0.2f s (%0.2f MBps)" %  (arg_dict['filepath'],arg_dict['split'] ,arg_dict['num_processes'], Uploadsize, t2, Uploadsize/t2))
	    else:
	   	 shutil.rmtree(Upload_dir_Name)
		 logger.error("sample=%0.2f , compress = %0.2f ,reduce rate %0.2fM  File %s part size = %d ,concurrency = %d ,upload (ori)%0.2fM in %0.2f s (%0.2f MBps)  proftime= %0.2f" %  (tsample,t3,rate,arg_dict['filepath'],arg_dict['split'] ,arg_dict['num_processes'], Uploadsize, t2, Uploadsize/t2, tp))
	    print "finish"
	    for mp in bucket.list_multipart_uploads():
		    mp.cancel_upload()
