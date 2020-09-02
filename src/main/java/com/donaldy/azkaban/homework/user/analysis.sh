#!/bin/sh

echo 'analysis user click。。。'

currDate=`date +%Y-%m-%d`

echo "现在时间：'$currDate'"

/opt/lagou/servers/hive-2.3.7/bin/hive -f hive.sql