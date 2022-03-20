#!/bin/bash
#create time: 2022-03-20 21:46:52
TIMEOUT="timeout -k 2s 180s "
rm -rf mr-tmp-test
mkdir mr-tmp-test || exit 1
cd mr-tmp-test || exit 1

#########################################################
echo '***' Starting crash test.

# generate the correct output
../cmd/mrsequential ../mrapps/nocrash.so ../data/pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
($TIMEOUT ../cmd/mrcoordinator ../data/pg*txt ; touch mr-done ) &
sleep 1

# start multiple workers
$TIMEOUT ../cmd/mrworker ../mrapps/crash.so &

# mimic rpc.go's coordinatorSock()
SOCKNAME=/var/tmp/824-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    $TIMEOUT ../cmd/mrworker ../mrapps/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    $TIMEOUT ../cmd/mrworker ../mrapps/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  $TIMEOUT ../cmd/mrworker ../mrapps/crash.so
  sleep 1
done

wait

rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi


