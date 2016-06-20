#!/bin/bash
i=0
while [ "$i" != "100" ]
do

#./s3-mp-upload.py -f ~/IOTtest/poty2006/ s3://us-east-1-lin/poty/
#./s3-mp-upload.py -f ~/IOTtest/java1.6/ s3://us-east-1-lin/Java6/
#./s3-mp-upload.py -f ~/IOTtest/java1.8/ s3://us-east-1-lin/Java8/
#./s3-mp-upload.py -f ~/IOTtest/Jay.avi s3://us-east-1-lin/Jay.avi
#./s3-mp-upload.py -n 1 -s 20 -f ~/IOTtest/Jay4g s3://us-east-1-lin/Jay
#./s3-mp-upload.py -n 1 -s 40 -f ~/IOTtest/Jay4g s3://us-east-1-lin/Jay
#./s3-mp-upload.py -n 1 -s 60 -f ~/IOTtest/Jay4g s3://us-east-1-lin/Jay
#./s3-mp-upload.py -n 1 -s 80 -f ~/IOTtest/Jay4g s3://us-east-1-lin/Jay
#./s3-mp-upload.py -n 1 -s 160 -f ~/IOTtest/Jay4g s3://us-east-1-lin/Jay
#./s3-mp-upload.py -n 1 -s 120 -f ~/IOTtest/Jay4g s3://us-east-1-lin/Jay
	i=$(($i+1))


done
s3cmd del s3://us-east-1-lin/*

