#!/bin/bash
grep "Uploaded part" 160bigmultipart.log  |awk -F"at " '{ print $2 }' |awk -F" " '{ print $1 }' >160upload
grep "Uploaded part" 120bigmultipart.log  |awk -F"at " '{ print $2 }' |awk -F" " '{ print $1 }' >120upload
grep "Uploaded part" 80bigmultipart.log  |awk -F"at " '{ print $2 }' |awk -F" " '{ print $1 }' >80upload
grep "Uploaded part" 60bigmultipart.log  |awk -F"at " '{ print $2 }' |awk -F" " '{ print $1 }' >60upload 
#grep "Uploaded part" 40bigmultipart.log  |awk -F"at " '{ print $2 }' |awk -F" " '{ print $1 }' >40upload 
grep "Uploaded part" 20bigmultipart.log  |awk -F"at " '{ print $2 }' |awk -F" " '{ print $1 }' >20upload

s3cmd put 160upload s3://us-east-1-lin
s3cmd put 120upload s3://us-east-1-lin
s3cmd put 80upload s3://us-east-1-lin
s3cmd put 60upload s3://us-east-1-lin
#s3cmd put 40upload s3://us-east-1-lin
s3cmd put 20upload s3://us-east-1-lin
