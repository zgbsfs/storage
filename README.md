# storage

Installation
--------------------
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

[boto3](https://boto3.readthedocs.org/en/latest/guide/quickstart.html#configuration)
[boto](http://boto.cloudhackers.com/en/latest/getting_started.html#configuring-boto-credentials)

Usage
-----

Upload directory:

```sh
$ ./s3-mp-upload.py -f /The/directory/with'/'/ s3://yourbucket/keyname/ 
```

Upload file:

```sh
$ ./s3-mp-upload.py -f /The/file/path/without'/' s3://yourbucket/keyname
```

Download:

```sh
$ ./s3-mp-upload.py -get s3://yourbucket/your/file/path /The/path/you/download
```
