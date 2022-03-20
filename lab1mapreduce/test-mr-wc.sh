#!/bin/bash
#create time: 2022-03-20 17:29:34

TIMEOUT="timeout -k 2s 180s "
rm -rf mr-tmp-wc
mkdir mr-tmp-wc || exit 1
cd mr-tmp-wc || exit 1
rm -f mr-*
#########################################################
# first word-count

# generate the correct output
../cmd/mrsequential ../mrapps/wc.so ../data/pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

$TIMEOUT ../cmd/mrcoordinator ../data/pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1
# start multiple workers.
$TIMEOUT ../cmd/mrworker ../mrapps/wc.so &
$TIMEOUT ../cmd/mrworker ../mrapps/wc.so &
$TIMEOUT ../cmd/mrworker ../mrapps/wc.so &

wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi
# wait for remaining workers and coordinator to exit.
wait

