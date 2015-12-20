#!/usr/bin/env python
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

import getfile
from threading import Thread,Lock,currentThread
from traffic import initargs
import numpy
parser = argparse.ArgumentParser(description="Transfer large files to S3",
        prog="s3-mp-upload")
parser.add_argument("filepath",  help="The file to transfer")
parser.add_argument("dest", help="The S3 destination object")
parser.add_argument("-np", "--num-processes", help="Number of processors to use",
        type=int, default=2)
parser.add_argument("-f", "--force", help="Overwrite an existing S3 key",
        action="store_true")
parser.add_argument("-s", "--split", help="Split size, in Mb", type=int, default=50)
parser.add_argument("-Thres", "--Threshold", help="compression size", type=int)
parser.add_argument("-rrs", "--reduced-redundancy", help="Use reduced redundancy storage. Default is standard.", default=False,  action="store_true")
parser.add_argument("--insecure", dest='secure', help="Use HTTP for connection",
        default=True, action="store_false")
parser.add_argument("-t", "--max-tries", help="Max allowed retries for http timeout", type=int, default=5)
parser.add_argument("-S", "--simulate", help="Enable simulation with the time per error ",type=int ,default=0)
parser.add_argument("-get",help="the path ", default=False,  action="store_true")

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

def handler(signum, frame):
	raise UserWarning("failure occur!!")
sp=0
status_list=None
New_Threadnum=0
timeout_event=None
#@timeout(sp/25, os.strerror(errno.ETIMEDOUT))
#@timeout(5, os.strerror(errno.ETIMEDOUT))
def do_part_upload(args,current_tries=1):
    """
    Upload a part of a MultiPartUpload

    Open the target file and read in a chunk. Since we can't pickle
    S3Connection or MultiPartUpload objects, we have to reconnect and lookup
    the MPU object with each part upload.

    :type args: tuple of (string, string, string, int, int, int)
    :param args: The actual arguments of this method. Due to lameness of
                 multiprocessing, we have to extract these outside of the
                 function definition.

                 The arguments are: S3 Bucket name, MultiPartUpload id, file
                 name, the part number, part offset, part size
    """
    # Multiprocessing args lameness
    bucket_name, mpu_id, fname, i, start, size, secure, max_tries, current_tries = args
    #print args
    logger.debug("do_part_upload got args: %s" % (args,))
    # Connect to S3, get the MultiPartUpload
    s3 = S3Connection()
    s3.is_secure = secure
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
        t1 = time.time()
	mpu.upload_part_from_file(StringIO(data), i)
        t2 = time.time() - t1
        s = len(data)/1024./1024.
	logger.info("Uploaded part %s (%0.2fM) in %0.3f s at %0.2f MBps" % (i, s, t2, s/t2))
    except KeyboardInterrupt:
	print "detect in here"
    except Exception, err:
	current_tries = current_tries+1
	#traceback.print_exc()
	logger.info(err)
        logger.info("Retry request %d of max %d times" % (current_tries, max_tries))
	if current_tries <max_tries:
	        do_part_upload(args,current_tries)
	else:
		return


def Thread_Upload(uploadFileNames,FileList,status_list,lock,filepath, dest, num_processes=2, split=50, force=False, reduced_redundancy=False, secure=False, max_tries=5, simulate=0,Threshold=0,get=False):

    #this is work  print currentThread().getName()
    # Check that dest is a valid S3 url
    lock.acquire()
    status_list[FileList.index(uploadFileNames)] = 'upload'
    lock.release()
    print "inthread "  + status_list[FileList.index(uploadFileNames)]
    split_rs = urlparse.urlsplit(dest)
    filepath = uploadFileNames
    src = open(filepath, "rw+")

    filepath = uploadFileNames.replace(os.getcwd(),"")
    print num_processes
    print split

    global sp
    if split>160:
       sp=split
    else:
       sp=160


    s3 = S3Connection()
    s3.is_secure = secure

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
    if size < split*2*1024*1024:
	global Threadnum
	print src.name +'  single use ' +str (num_processes)
	src.seek(0)
	t1 = time.time()
	k = boto.s3.key.Key(bucket,filepath)
	k.set_contents_from_file(src)
	t2 = time.time() - t1
	s = size/1024./1024.
	'''
	hdlr = logging.FileHandler("bigmultipart.log")
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	hdlr.setFormatter(formatter)
	#logger.addHandler(hdlr)
	#logger.info("part size = %d ,concurrency = %d ,Finished uploading %0.3f M in %0.3f s (%0.3f MBps)" % (split ,num_processes, s, t2, s/t2))
	'''

	lock.acquire()
	status_list[FileList.index(uploadFileNames)]=-1
	print src.name +" finish  "+str (status_list)
	critical_threadnum(1)
	print "finish " + uploadFileNames +" add back now is   " + str(Threadnum)
	lock.release()
	#os.remove(src.name)
	return

    # Create the multi-part upload object
    mpu = bucket.initiate_multipart_upload(filepath, reduced_redundancy=reduced_redundancy)
    #logger.info("Initialized upload: %s" % mpu.id)
    
    # Generate arguments for invocations of do_part_upload
    def gen_args(x, fold_last, upload_list):
        for i in upload_list:
            part_start = part_size*(i-1)
	    if i == num_parts and fold_last is True:
	    	print "fold_last" +src.name
                yield (bucket.name, mpu.id, src.name, i, part_start, part_size+10*1024*1024, secure, max_tries, 0)
	    else:
                yield (bucket.name, mpu.id, src.name, i, part_start, part_size, secure, max_tries, 0)
    # If the last part is less than 5M, just fold it into the previous part
    fold_last = ((size % part_size) < split*1024*1024)
    upload_list=[]
    for i in range(1,num_parts+1):   
	    upload_list.append(i)
    # Do the thing
    t1 = time.time()
    def master_progress(mpu,num_processes,bucket,upload_list):
	    print "fucke "
	    x=0
	    x= len(mpu.get_all_parts())
	    for i in mpu.get_all_parts():
		a=int(str(i).split(" ")[1].split(">")[0])
		if a in upload_list:
			upload_list.remove(a)
	    while True:

		try:
			print "aaaaa"
			if x!=num_parts:
	#			print "mpu.id = "+str(mpu.id) +"  is "+src.name +"  use " +str(num_processes)
				status_list[FileList.index(uploadFileNames)]=mpu.id	
	#			print str(x) +" = master proce "  + status_list[FileList.index(uploadFileNames)]
								
				pool = Pool(processes=num_processes)
				pool.map_async(do_part_upload, gen_args(x,fold_last,upload_list)).get(99999999)

                        src.close()
                        mpu.complete_upload()
			print "mpu.complete src name " +src.name
			#os.remove(src.name)
			global Threadnum
			lock.acquire()
			status_list[FileList.index(uploadFileNames)]=-1
			print src.name +" finish  "+str (status_list)
			critical_threadnum(num_processes)
			print uploadFileNames +" add back now is   " + str(Threadnum)
			lock.release()
			pool.terminate()
                        break
		except KeyboardInterrupt:
			logger.warn("Received KeyboardInterrupt, canceling upload")
			pool.terminate()
			mpu.cancel_upload()
			print "keyboarddddddddddddddddddddddddddddddd"
			break
		except Exception, err:
			logger.error("Encountered an error, canceling upload aaaaaaaaaaaa")
			print src.name
			logger.error(err)
			break
			#pool.terminate()
			traceback.print_exc()
			print mpu.get_all_parts()
			x= len(mpu.get_all_parts())
			for i in mpu.get_all_parts():
				a=int(str(i).split(" ")[1].split(">")[0])
				#    print "what to del " + str(a)
				if a in upload_list:
					upload_list.remove(a)
			print "exception :upload " +str(x) +"parts"
			logger.error(err)
			if simulate!=0:
				#signal.alarm(int(random.expovariate(1.0/120.0)))
				signal.alarm(7)
			break

    t1 = time.time()
    master_progress(mpu,num_processes,bucket,upload_list)

def getBandwidth():
	proc = subprocess.Popen('speedtest-cli', stdout=subprocess.PIPE)

	strlist = proc.stdout.read().split(" ")
	print "bandwidth = "+ strlist[len(strlist)-2]
	return  strlist[len(strlist)-2]

def critical_threadnum(addback):
	global Threadnum
	global New_Threadnum
	NewValue = New_Threadnum
        if Threadnum +addback > NewValue:
		Threadnum = NewValue
        else:
		Threadnum +=addback

def FIFO(arg_dict,uploadFileNames,bandwidth):
    manager = Manager()
#    status_list = Array('i', range(len(uploadFileNames)))
    status_list = manager.list(range(len(uploadFileNames)))
    global Threadnum 
    lock = Lock()
    i = 0
    toThread = arg_dict.copy()
    change_thread  = Process(name='ChangeResource',target=NewResourceSize,args=(Threadnum,lock,bandwidth,status_list))
    change_thread.daemon = True
	
    change_thread.start() 
#    change_thread.terminate()

#    if arg_dict['simulate']!=0:
#	    signal.alarm(int(random.expovariate(1.0/simulate)))
    #signal.alarm(50)
    #signal.signal(signal.SIGALRM, handler)
    Thread_list=[]
    Failure = 0
    dict_list = list((i)for i in range(len(uploadFileNames)))
    while True:
	    try:
		    if status_list.count(-1)==len(uploadFileNames):
			    change_thread.terminate()
			    break
		    if i == len(uploadFileNames):
			    i=0
			    while status_list[i]!=i:
				    if i == len(uploadFileNames)-1:
					    i=0
				    else:
					    i+=1
		    if Threadnum >0 and status_list[i]==i :
			    print "e04    "+ str(status_list[i]) +"    "+str(i)
			    print "one time "
			    if dict_list[i]==i:
				    toThread['filepath'] ,toThread['num_processes'] = FIFOargs(uploadFileNames,i,arg_dict['split'],arg_dict['num_processes'])
				    toThread['split']-=Failure
				    dict_list[i] = toThread
			    configure = dict_list[i]
				   
			    if Threadnum != 0  and Threadnum < toThread['num_processes']:
				    i+=1
				    if status_list[i]==i:#status_list[i]!=-1 or status_list[i]!='upload':
					    continue
			    func = partial(Thread_Upload,**configure)
			    a = Process(name=uploadFileNames[i],target=func,args=(uploadFileNames[i],uploadFileNames,status_list,lock))
			    a.daemon =False
			    a.start()
			    Threadnum -= toThread['num_processes']
			    i+=1
		    else:
			    i+=1
		
	    except IndexError:
		pass
	    except KeyboardInterrupt:
		print "FIFO ^c"
		break
	    except UserWarning:
		Failure +=1
		print active_children()
		for task_process in  active_children():
			task_process.terminate()
		for i in range(len(status_list)):
                        if status_list[i]=='upload':
                                status_list[i]=i
		Threadnum = 10	
		print "Dont plerase" + str(active_children())
		print status_list
		#while active_children():
		#	print "have" +str(active_children())
		#	task_process.terminate()
		#signal.alarm(50)


def FIFOargs(uploadFileNames,now,split,inputProcessNum):
	sizeinMB = os.path.getsize(uploadFileNames[now])/1024./1024.
	num_process = int(ceil(sizeinMB/split))
	if inputProcessNum < num_process:
		num_process = inputProcessNum
#	print uploadFileNames[now],num_process
	return uploadFileNames[now],num_process
def NewResourceSize(originThreadnum,lock,bandwidth,status_list):
	global New_Threadnum
	global Threadnum
	New_Threadnum = Threadnum
	last_Threadnum = originThreadnum
	last_throughput = bandwidth
	counter = 0
	time.sleep(4)
	history_throughput = []
	STD = 50
	while status_list.count(-1)!=len(uploadFileNames):
		try:
			if STD>1:
				Unstable = True
			else:
				print "stable throughput =  "+str (numpy.median(history_throughput))
				Unstable = False
				break
			if Unstable:
				now_throughput = initargs(5,)
				now_throughput = now_throughput/1024./1024.
				if len(history_throughput)>5 :
					print "WTFFFF  " +str(numpy.std(history_throughput))
					STD = float(numpy.std(history_throughput))

					print "standard XXXXX " + str(STD)
					history_throughput.pop()
					history_throughput.insert(0,now_throughput)
				else:
					history_throughput.insert(0,now_throughput)

				if now_throughput < last_throughput/2 or counter>3:
					print str(last_throughput) + " is higher than "+str(now_throughput)+" ,increase resource "
					
					New_Threadnum +=New_Threadnum
					last_throughput = now_throughput
					counter = 0
				else:
					
					counter+=1
				if New_Threadnum > last_Threadnum :
					lock.acquire()
					Threadnum +=New_Threadnum-last_Threadnum
					lock.release()
					print "resource increase "+str(New_Threadnum-last_Threadnum)+" now is " +str(Threadnum)
					last_Threadnum = New_Threadnum
					print "change to New_Threadnum " +str(New_Threadnum) 	
			else:
				initargs(10,)
		except KeyboardInterrupt:
			print "NewResource ^c"
			break
	print "stop please"

if __name__ == "__main__":
    #logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    arg_dict = vars(args)
    split_rs = urlparse.urlsplit(arg_dict['dest'])
    if split_rs.scheme != "s3":
	    raise ValueError("is not an S3 url")
    s3 = S3Connection()
    bucket = s3.get_bucket(split_rs.netloc)
    if bucket == None:
	    raise ValueError("'%s' is not a valid bucket" % split_rs.netloc)

    if arg_dict['get']:
	    getfile.GetTheFile(arg_dict['filepath'])
    else:
	    hdlr = logging.FileHandler("bigmultipart.log")
	    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	    hdlr.setFormatter(formatter)
	    logger.addHandler(hdlr)
	    logger.setLevel(logging.ERROR)
	   	
	   # bandwidth = float(Thread(name='ChangeResource',target=getBandwidth,args=('something',)))	    
	   # print "bandwidth = "+ str(bandwidth)

	    filepath = arg_dict['filepath']
	    print arg_dict
	    # it work for du -sh *,get the size
	    print filepath
	    proc = subprocess.Popen(['du','-sk',filepath], stdout=subprocess.PIPE)
	    strlist = proc.stdout.read().split("\t")
	    print 'origin size is ' +str(strlist[0])
	    Uploadsize = float(strlist[0])

	    uploadFileNames = []
	    Upload_dir_Name = os.getcwd()+split_rs.path
	    t1 = time.time()

	    if os.path.isfile(filepath):
		Upload_dir_Name ,nocompression= compress.CompressBigFile(filepath,split_rs.path,filepath,Upload_dir_Name,False)
		uploadFileNames.append(Upload_dir_Name)
		afterCompressSize = 0
		Uploadsize = os.path.getsize(Upload_dir_Name)/1024.
	    else:
		GZfile,METAfile,afterCompressSize = compress.Compression(filepath,split_rs.path,arg_dict['Threshold'])
		uploadFileNames = GZfile+METAfile
	
	    '''

	   # print int(int(bandwidth)/int(arg_dict['split']))
	    uploadFileNames = ['/Users/ytlin/GitHub/storage/1g/1_data.gz', '/Users/ytlin/GitHub/storage/1g/2_data.gz', '/Users/ytlin/GitHub/storage/1g/3_data.gz', '/Users/ytlin/GitHub/storage/1g/4_data.gz', '/Users/ytlin/GitHub/storage/1g/1_data.meta', '/Users/ytlin/GitHub/storage/1g/2_data.meta', '/Users/ytlin/GitHub/storage/1g/3_data.meta', '/Users/ytlin/GitHub/storage/1g/4_data.meta']
	    



	    '''
	    t3 = time.time() - t1
	    global Threadnum 
	    bandwidth = 20/8
	    Threadnum =  max( 10 ,int(bandwidth/int(arg_dict['split'])))
#	    Threadnum = int(20/int(arg_dict['split']))
#	    Threadnum = 10
	    print Threadnum
#	    uploadDict = dict((Filepath ,'new') for Filepath in uploadFileNames)
#	    store = arg_dict['num_processes']



	    print uploadFileNames 
	    FIFO(arg_dict,uploadFileNames,bandwidth)

	    t2 = time.time() - t1
	    print "total time= "+str(t2)
	    Uploadsize = Uploadsize/1024.
	    afterCompressSize = afterCompressSize/1024./1024.
	    rate = (Uploadsize -afterCompressSize) /t3
	    if os.path.isfile(filepath) and nocompression:
		 logger.error("Nocompression  File %s part size = %d ,concurrency = %d ,upload (ori)%0.2fM in %0.2f s (%0.2f MBps)" %  (arg_dict['filepath'],arg_dict['split'] ,arg_dict['num_processes'], Uploadsize, t2, Uploadsize/t2))
	    else:
	   	 shutil.rmtree(Upload_dir_Name)
		 logger.error("compress = %0.2f ,reduce rate %0.2fM  File %s part size = %d ,concurrency = %d ,upload (ori)%0.2fM in %0.2f s (%0.2f MBps)" %  (t3,rate,arg_dict['filepath'],arg_dict['split'] ,arg_dict['num_processes'], Uploadsize, t2, Uploadsize/t2))
	    print "finish"
	    for mp in bucket.list_multipart_uploads():
		    mp.cancel_upload()
	   # main(**arg_dict)
