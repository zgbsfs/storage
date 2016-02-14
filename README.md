# storage


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
