#!/usr/bin/env bash

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1

failed_any=0
 
printf "\u001bc"

# # Word Count
# ../mrsequential ../apps/wordcount.jar WordCount ../data/pg*txt || exit 1
# sort mr-out-0 > mr-correct-wc.txt
# rm -f mr-out*

# echo '***' Starting wc test.

# # start multiple workers.
# timeout -k 2s 180s ../mrworker ../apps/wordcount.jar WordCount 0 3 &
# timeout -k 2s 180s ../mrworker ../apps/wordcount.jar WordCount 1 3 &
# timeout -k 2s 180s ../mrworker ../apps/wordcount.jar WordCount 2 3 &

# # wait for workers
# sleep 10

# timeout -k 2s 180s ../mrcoordinator 3 ../data/pg*txt &
# pid=$!

# # wait for the coordinator to exit.
# wait $pid

# # since workers are required to exit when a job is completely finished,
# # and not before, that means the job has finished.
# sort mr-out* | grep . > mr-wc-all
# if cmp mr-wc-all mr-correct-wc.txt
# then
#   echo '---' wc test: PASS
# else
#   echo '---' wc output is not the same as mr-correct-wc.txt
#   echo '---' wc test: FAIL
#   failed_any=1
# fi

# # wait for remaining workers and coordinator to exit.
# wait

# #########################################################






#########################################################
echo '***' Starting map parallelism test.

rm -f mr-*

timeout -k 2s 180s ../mrworker ../apps/map-parallel.jar MapParallel 0 2 &
timeout -k 2s 180s ../mrworker ../apps/map-parallel.jar MapParallel 1 2 &

# wait for workers
sleep 10

timeout -k 2s 180s ../mrcoordinator 2 ../data/pg*txt &

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