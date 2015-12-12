#!/usr/bin/env python
import argparse
import os
import errno
from cStringIO import StringIO
import logging
from math import ceil
import multiprocessing
import multiprocessing.pool
import time
import urlparse
from timeout import timeout
import boto
from boto.s3.connection import S3Connection
import random
import sys, traceback
import signal
import compress
from functools import partial
import subprocess
import getfile
from threading import Thread,Lock

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

class NoDaemonProcess(multiprocessing.Process):
# make 'daemon' attribute always return False
    def _get_daemon(self):
    	return False
    def _set_daemon(self, value):
	pass
    daemon = property(_get_daemon, _set_daemon)

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class NoDaemonProcessPool(multiprocessing.pool.Pool):
	Process = NoDaemonProcess

def handler(signum, frame):
	raise Exception("failure occur!!")
sp=0
Threadnum = None

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
	return fname
    except Exception, err:
	current_tries = current_tries+1
	#traceback.print_exc()
	logger.info(err)
        logger.info("Retry request %d of max %d times" % (current_tries, max_tries))
	if current_tries <max_tries:
	        do_part_upload(args,current_tries)
	else:
		return


def main(uploadFileNames,lock,filepath, dest, num_processes=2, split=50, force=False, reduced_redundancy=False, secure=False, max_tries=5, simulate=0,Threshold=0,get=False):
    """
    global Threadnum
    test = random.randint(4,8)
    print  "Total = "  + str(Threadnum) +" threads "+ uploadFileNames + "  how much thread =" +str(num_processes) +" sleep time ="+str(test)
    time.sleep(test)  
    lock.acquire()
    global Threadnum 
    Threadnum +=num_processes
    print uploadFileNames +" add back now is   " + str(Threadnum)
    lock.release()
    return uploadFileNames
    """
    # Check that dest is a valid S3 url
    split_rs = urlparse.urlsplit(dest)
    filepath = uploadFileNames
    src = open(filepath, "rw+")

    filepath = uploadFileNames.replace(os.getcwd(),"")
    print filepath

    if split_rs.scheme != "s3":
        raise ValueError("'%s' is not an S3 url" % dest)
    global sp
    if split>160:
       sp=split
    else:
       sp=160


    s3 = S3Connection()
    s3.is_secure = secure
    bucket = s3.get_bucket(split_rs.netloc)
    if bucket == None:
        raise ValueError("'%s' is not a valid bucket" % split_rs.netloc)
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
	if num_processes !=1:
		lock.acquire()
		Threadnum +=num_processes -1
		print uploadFileNames +" add " + str(num_processes -1)+  " because single part , now is   " + str(Threadnum)
		lock.release()

        src.seek(0)
        t1 = time.time()
        k = boto.s3.key.Key(bucket,filepath)
        k.set_contents_from_file(src)
        t2 = time.time() - t1
        s = size/1024./1024.
	hdlr = logging.FileHandler("bigmultipart.log")
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
	logger.info("part size = %d ,concurrency = %d ,Finished uploading %0.3f M in %0.3f s (%0.3f MBps)" % (split ,num_processes, s, t2, s/t2))

	print  "Total = "  + str(Threadnum) +" threads "+ uploadFileNames + "  how much thread =" +str(num_processes) 
	lock.acquire()
	Threadnum +=1
	print "finish " + uploadFileNames +" add back now is   " + str(Threadnum)
	lock.release()
	return uploadFileNames


    # Create the multi-part upload object
    mpu = bucket.initiate_multipart_upload(filepath, reduced_redundancy=reduced_redundancy)
    logger.info("Initialized upload: %s" % mpu.id)
    
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
	    x=0
	    while True:
		try:
			if x!=num_parts:
				print "Hi"
				pool = NoDaemonProcessPool(processes=num_processes)
				pool.map_async(do_part_upload, gen_args(x,fold_last,upload_list)).get(99999999)
                        src.close()
	#		print "finish" +str(mpu.get_all_parts())
                        mpu.complete_upload()
                        t2 = time.time() - t1
                        s = size/1024./1024.
			#print str(bucket) + str(s) + str(t2) + str(s/t2)
                        logger.info("bucket %s part size = %d ,concurrency = %d ,Finished uploading %0.3fM in %0.3f s (%0.3f MBps)" %  (bucket,split ,num_processes, s, t2, s/t2))
			global Threadnum
			print  "Total = "  + str(Threadnum) +" threads "+ uploadFileNames + "  how much thread =" +str(num_processes) 
			lock.acquire()
			Threadnum +=num_processes
			print uploadFileNames +" add back now is   " + str(Threadnum)
			lock.release()
                        break
		except KeyboardInterrupt:
			logger.warn("Received KeyboardInterrupt, canceling upload")
			pool.terminate()
			mpu.cancel_upload()
			break
		except Exception, err:
			logger.error("Encountered an error, canceling upload aaaaaaaaaaaa")
			print src.name
			logger.error(err)
			pool.terminate()
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
			#break

    t1 = time.time()
    #signal.signal(signal.SIGALRM, handler)
    #print mpu.get_all_parts()
    print simulate
    if simulate!=0:
    	signal.alarm(int(random.expovariate(1.0/simulate)))
   #    signal.alarm(10)
    master_progress(mpu,num_processes,bucket,upload_list)
def getBandwidth():
	proc = subprocess.Popen('speedtest-cli', stdout=subprocess.PIPE)

	strlist = proc.stdout.read().split(" ")
	print "bandwidth = "+ strlist[len(strlist)-2]
	return  strlist[len(strlist)-2]

def gen_argsfor(x, fold_last, upload_list):
	return 
def FIFO():
	return 
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    arg_dict = vars(args)
    print arg_dict['get']
    if arg_dict['get']:
	    getfile.GetTheFile(arg_dict['filepath'])
    else:
	    logger.debug("CLI args: %s" % args)
	    hdlr = logging.FileHandler("bigmultipart.log")
	    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
	    hdlr.setFormatter(formatter)
	    logger.addHandler(hdlr)
	    logger.setLevel(logging.ERROR)
	    bandwidth = float(getBandwidth())
	    
	    print "bandwidth = "+ str(bandwidth)

	    split_rs = urlparse.urlsplit(arg_dict['dest'])
	    filepath = arg_dict['filepath']
	    print arg_dict
	    uploadFileNames = []
	    new_path = os.getcwd()+split_rs.path
	    t1 = time.time()

	    if os.path.isfile(filepath):
		new_path = compress.CompressBigFile(filepath,split_rs.path,filepath,new_path,False)
		uploadFileNames.append(new_path)
		s += os.path.getsize(new_path)
	    else:
		GZfile,METAfile,Uploadsize = compress.Compression(filepath,split_rs.path,arg_dict['Threshold'])
		uploadFileNames = GZfile+METAfile
	    """
	    print int(bandwidth/int(arg_dict['split']))
	    uploadFileNames = ['/Users/ytlin/GitHub/storage/1g/1_data.gz', '/Users/ytlin/GitHub/storage/1g/2_data.gz', '/Users/ytlin/GitHub/storage/1g/3_data.gz', '/Users/ytlin/GitHub/storage/1g/4_data.gz', '/Users/ytlin/GitHub/storage/1g/5_data.gz', '/Users/ytlin/GitHub/storage/1g/6_data.gz', '/Users/ytlin/GitHub/storage/1g/7_data.gz', '/Users/ytlin/GitHub/storage/1g/8_data.gz', '/Users/ytlin/GitHub/storage/1g/1_data.meta', '/Users/ytlin/GitHub/storage/1g/2_data.meta', '/Users/ytlin/GitHub/storage/1g/3_data.meta', '/Users/ytlin/GitHub/storage/1g/4_data.meta', '/Users/ytlin/GitHub/storage/1g/5_data.meta', '/Users/ytlin/GitHub/storage/1g/6_data.meta', '/Users/ytlin/GitHub/storage/1g/7_data.meta', '/Users/ytlin/GitHub/storage/1g/8_data.meta']
	    """
	    t3 = time.time() - t1
	    global Threadnum 
	    Threadnum =  int(bandwidth/int(arg_dict['split']))
	    lock = Lock() 
	    i=0
	
	    store = arg_dict['num_processes']
	    
	    proc = subprocess.Popen(['du','-sh',filepath], stdout=subprocess.PIPE)
            strlist = proc.stdout.read().split("\t")
            print strlist[0]
	    Thread_list = []	
	    while True:
		    if Threadnum >0 and i != len(uploadFileNames)-1:
			    if arg_dict['num_processes'] > Threadnum and Threadnum != 0:
				    arg_dict['num_processes'] = Threadnum
	#		    else:
	#		    	    pass
			    func = partial(main,**arg_dict)
			    a = Thread(target=func,args=(uploadFileNames[i],lock))
			    a.setName(uploadFileNames[i])
			    a.start()
			    Thread_list.append(a)
			    i+=1
	#		    if not a.isAlive():
	#		    	print a.getName()
			    Threadnum -= arg_dict['num_processes']
			    arg_dict['num_processes'] = store
			    print Threadnum 	 
		    for thread in Thread_list:
			    if not thread.isAlive():
				    Thread_list.remove(thread)
		    if not Thread_list:
			    print "breadk ????"
			    break
		    """
		    if i == len(uploadFileNames)-1 :
			    print "breadk ????"
			    break
		    """
	    """
	    Mainpool = NoDaemonProcessPool(processes=2)
	    func = partial(main,**arg_dict)
	    Mainpool.map_async(func,uploadFileNames).get(99999999)
	    """
	    t2 = time.time() - t1
	    print "total time= "+str(t2)
	    s3 = S3Connection()
	    bucket = s3.get_bucket(split_rs.netloc)
	    Uploadsize = Uploadsize/1024/1024
	    logger.error("compress_time = %0.3f ,File %s part size = %d ,concurrency = %d ,Finished uploading %0.3fM in %0.3f s (%0.3f MBps)" %  (t3,arg_dict['filepath'],arg_dict['split'] ,arg_dict['num_processes'], Uploadsize, t2, Uploadsize/t2))
	    print "finish"
	    for mp in bucket.list_multipart_uploads():
		    mp.cancel_upload()
#	    Mainpool.terminate()
	   # main(**arg_dict)
