Installation
--------------------

speedtest-cli
```
$ sudo easy_install speedtest-cli
```

python-magic
```
$ sudo easy_install magic
```

boto3
```
$ sudo easy_install boto3
```

boto
```
$ sudo easy_install boto
```

gcc
```
$ sudo yum install gcc
```

psutil
```
$ sudo easy_install psutil
```

numpy
```
$ sudo easy_install numpy
```

configuration boto,boto3
------------------------

[boto](http://boto.cloudhackers.com/en/latest/getting_started.html#configuring-boto-credentials)
[boto3](https://boto3.readthedocs.org/en/latest/guide/quickstart.html#configuration)

Usage
-----

```
usage: s3-mp-upload [-h] [-n NUM_PROCESSES] [-f] [-s SPLIT] [-Thres THRESHOLD]
                    [-S SIMULATE] [-get] [--Retransmit]
                    filepath dest

Transfer large files to S3

positional arguments:
  filepath              The file to transfer
  dest                  The S3 destination object

optional arguments:
  -h, --help            show this help message and exit
  -n NUM_PROCESSES, --num-processes NUM_PROCESSES
                        Number of processors to use
  -f, --force           Overwrite an existing S3 key
  -s SPLIT, --split SPLIT
                        Split size, in Mb
  -Thres THRESHOLD, --Threshold THRESHOLD
                        compression size
  -S SIMULATE, --simulate SIMULATE
                        Enable simulation with the time per error
  -get                  download the single file from s3 path
  --Retransmit          Enable Retransmition simulation
```

Upload directory:

```sh
$ ./s3-mp-upload.py -f /The/directory/with'/'/ s3://yourbucket/keyname/
```
```
if you want to upload the hold directory *example* ,to bucket *us-east-1* ,named *ex*
```
```
$ ./s3-mp-upload.py -f /example/ s3://us-east-1/ex/ 
```

Upload file:

```sh
$ ./s3-mp-upload.py -f /The/file/path/without'/' s3://yourbucket/keyname

if you want to upload the file *file* ,to bucket *us-east-1* ,named *singlefile*
$ ./s3-mp-upload.py -f /file s3://us-east-1/singlefile
```

Download: (now only support single file download)

```sh
$ ./s3-mp-upload.py -get s3://yourbucket/your/file/path /The/path/you/download

if you want to download the file /newname/a.jpg from bucket *us-east-1* ,to local "a.jpg"
$ ./s3-mp-upload.py -f  s3://us-east-1/newname/a.jpg  a.jpg
```
