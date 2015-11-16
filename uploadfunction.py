import logging
from multiprocessing import Pool
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
    bucket_name, mpu_id, fname, i, start, size, secure, max_tries, current_tries = args
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
        logger.info("Uploaded part %s (%0.2fM) in %0.3f s at %0.2f MBps" % (i, s, t2, s/t2))
    except Exception, err:
		#traceback.print_exc()
        logger.info(err)
        logger.info("Retry request %d of max %d times" % (current_tries, max_tries))
        do_part_upload(args)

# Generate arguments for invocations of do_part_upload
def gen_args(x, fold_last, upload_list):
	print upload_list
	for i in upload_list:
	    	part_start = part_size*(i-1)
    	if i == num_parts and fold_last is True:
    		yield (bucket.name, mpu.id, src.name, i, part_start, part_size+10*1024*1024, secure, max_tries, 0)
    	else:
	    	yield (bucket.name, mpu.id, src.name, i, part_start, part_size, secure, max_tries, 0)

def master_progress(mpu,num_processes,bucket,upload_list,num_parts,simulate):
    x=0
    while True:
	try:
		if x!=num_parts:

	    		pool = Pool(processes=num_processes)
			pool.map_async(do_part_upload, gen_args(x,fold_last,upload_list)).get(99999999)
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
		logger.info("bucket %s part size = %d ,concurrency = %d ,Finished uploading %0.3fM in %0.3f s (%0.3f MBps)" %  (bucket,split ,num_processes, s, t2, s/t2))
		break
	except KeyboardInterrupt:
		logger.warn("Received KeyboardInterrupt, canceling upload")
		pool.terminate()
		mpu.cancel_upload()
		break
	except Exception, err:
		logger.error("Encountered an error, canceling upload aaaaaaaaaaaa")
		pool.terminate()
#               traceback.print_exc()
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
