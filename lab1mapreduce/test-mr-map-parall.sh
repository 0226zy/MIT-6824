#!/bin/bash
#create time: 2022-03-20 21:41:44

TIMEOUT="timeout -k 2s 180s "
rm -rf mr-tmp-test
mkdir mr-tmp-test || exit 1
cd mr-tmp-test || exit 1
rm -f mr-*
#########################################################
echo '***' Starting map parallelism test.

rm -f mr-*

$TIMEOUT ../cmd/mrcoordinator ../data/pg*txt &
sleep 1

$TIMEOUT ../cmd/mrworker ../mrapps/mtiming.so &
$TIMEOUT ../cmd/mrworker ../mrapps/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait

