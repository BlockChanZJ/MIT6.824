#!/bin/bash 
 
for((i=1;i<=($2);i++)); 
do  
go test -run $1
done 
