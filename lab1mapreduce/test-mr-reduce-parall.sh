#!/bin/bash
#create time: 2022-03-20 21:44:28

TIMEOUT="timeout -k 2s 180s "
rm -rf mr-tmp-test
mkdir mr-tmp-test || exit 1
cd mr-tmp-test || exit 1
rm -f mr-*
#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

$TIMEOUT ../cmd/mrcoordinator ../data/pg*txt &
sleep 1

$TIMEOUT ../cmd/mrworker ../mrapps/rtiming.so &
$TIMEOUT ../cmd/mrworker ../mrapps/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait
