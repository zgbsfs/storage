#!/usr/bin/env python
import argparse
import os
import errno
from cStringIO import StringIO
import logging
from math import ceil
from multiprocessing import Pool
import time
import urlparse
from timeout import timeout
import boto
from boto.s3.connection import S3Connection
import random
import sys, traceback
import signal

parser = argparse.ArgumentParser(description="Transfer large files to S3",
        prog="s3-mp-upload")
parser.add_argument("src", type=file, help="The file to transfer")
parser.add_argument("dest", help="The S3 destination object")
parser.add_argument("-np", "--num-processes", help="Number of processors to use",
        type=int, default=2)
parser.add_argument("-f", "--force", help="Overwrite an existing S3 key",
        action="store_true")
parser.add_argument("-s", "--split", help="Split size, in Mb", type=int, default=50)
parser.add_argument("-rrs", "--reduced-redundancy", help="Use reduced redundancy storage. Default is standard.", default=False,  action="store_true")
parser.add_argument("--insecure", dest='secure', help="Use HTTP for connection",
        default=True, action="store_false")
parser.add_argument("-t", "--max-tries", help="Max allowed retries for http timeout", type=int, default=5)
parser.add_argument("-v", "--verbose", help="Be more verbose", default=False, action="store_true")
parser.add_argument("-q", "--quiet", help="Be less verbose (for use in cron jobs)", default=False, action="store_true")
parser.add_argument("-S", "--simulate", help="Enable simulation with the time per error ",type=int ,default=0)
logger = logging.getLogger("s3-mp-upload")

def handler(signum, frame):
	raise Exception("failure occur!!")
sp=0
#@timeout(sp/25, os.strerror(errno.ETIMEDOUT))
def do_part_upload(args):
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
    bucket_name, mpu_id, fname, i, start, size, secure, max_tries, current_tries ,simulate= args
    #print args
    logger.debug("do_part_upload got args: %s" % (args,))
    # Connect to S3, get the MultiPartUpload
    s3 = S3Connection()
    s3.is_secure = secure
    bucket = s3.get_bucket(bucket_name)
    mpu = None
    print "which process  " +str(i) 
    for mp in bucket.list_multipart_uploads():
        if mp.id == mpu_id:
            mpu = mp
            break
    	else:
            mp.cancel_upload()
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
	#print t2
        s = len(data)/1024./1024.
	logger.info("Uploaded part %s (%0.2fM) in %0.3f s at %0.2f MBps time interval= %d" % (i, s, t2, s/t2,simulate))
    except Exception, err:
	#traceback.print_exc()
	logger.info(err)
        logger.info("Retry request %d of max %d times" % (current_tries, max_tries))
#        do_part_upload(args)

def main(src, dest, num_processes=2, split=50, force=False, reduced_redundancy=False, verbose=False, quiet=False, secure=False, max_tries=5, simulate=120):
    # Check that dest is a valid S3 url
    split_rs = urlparse.urlsplit(dest)
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
    key = bucket.get_key(split_rs.path)
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
    if size < 5*1024*1024:
        src.seek(0)
        t1 = time.time()
        k = boto.s3.key.Key(bucket,split_rs.path)
        k.set_contents_from_file(src)
        t2 = time.time() - t1
        s = size/1024./1024.
	hdlr = logging.FileHandler("bigmultipart.log")
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
	logger.info("part size = %d ,concurrency = %d ,Finished uploading %0.3f M in %0.3f s (%0.3f MBps)" % (split ,num_processes, s, t2, s/t2))
        return

    # Create the multi-part upload object
    mpu = bucket.initiate_multipart_upload(split_rs.path, reduced_redundancy=reduced_redundancy)
    logger.info("Initialized upload: %s" % mpu.id)
    # Generate arguments for invocations of do_part_upload
    def gen_args(x, fold_last, upload_list,s):
	print upload_list
        for i in upload_list:
            part_start = part_size*(i-1)
            if i == num_parts and fold_last is True:
                yield (bucket.name, mpu.id, src.name, i, part_start, part_size+10*1024*1024, secure, max_tries, 0,s)
	    elif upload_list==None:
		print "finish  \n " +str(upload_list)
		break
            else:
                yield (bucket.name, mpu.id, src.name, i, part_start, part_size, secure, max_tries, 0,s)
	    '''
	    finished_list=mpu.get_all_parts()
	    for i in finished_list:
                a=int(str(i).split(" ")[1].split(">")[0])
            #    print "what to del " + str(a)
		if a in upload_list:
	                upload_list.remove(a)

	#	print upload_list
            '''
    # If the last part is less than 5M, just fold it into the previous part
    fold_last = ((size % part_size) < 10*1024*1024)
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
				pool = Pool(processes=num_processes)
				pool.map_async(do_part_upload, gen_args(x,fold_last,upload_list,simulate)).get(99999999)
                        src.close()
			print "finish" +str(mpu.get_all_parts())
                        mpu.complete_upload()
                        hdlr = logging.FileHandler("bigmultipart.log")
                        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
                        hdlr.setFormatter(formatter)
                        logger.addHandler(hdlr)
                        logger.setLevel(logging.INFO)
                        t2 = time.time() - t1
                        s = size/1024./1024.
                        logger.info("bucket %s part size = %d ,concurrency = %d ,Finished uploading %0.3fM in %0.3f s (%0.3f MBps) interval=%d" %  (bucket,split ,num_processes, s, t2, s/t2,simulate))
                        break
		except KeyboardInterrupt:
			logger.warn("Received KeyboardInterrupt, canceling upload")
			pool.terminate()
			mpu.cancel_upload()
			break
		except Exception, err:
			logger.error("Encountered an error, canceling upload aaaaaaaaaaaa")
			pool.terminate()
	#		traceback.print_exc()
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
				ti=int(random.expovariate(1.0/simulate))
				while ti==0:
					ti=int(random.expovariate(1.0/simulate))
				print ti
				signal.alarm(ti)
			#break

    t1 = time.time()
    #print mpu.get_all_parts()
    signal.signal(signal.SIGALRM, handler)
#    print simulate
    if simulate!=0:
	ti=int(random.expovariate(1.0/simulate))
        while ti==0:
                ti=int(random.expovariate(1.0/simulate))
        print ti
        signal.alarm(ti)
    master_progress(mpu,num_processes,bucket,upload_list)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = parser.parse_args()
    arg_dict = vars(args)
    if arg_dict['quiet'] == True:
        logger.setLevel(logging.WARNING)
    if arg_dict['verbose'] == True:
        logger.setLevel(logging.DEBUG)
    logger.debug("CLI args: %s" % args)
    main(**arg_dict)
