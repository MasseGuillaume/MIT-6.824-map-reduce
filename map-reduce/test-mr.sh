#!/usr/bin/env bash

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1

failed_any=0
 
printf "\u001bc"

#########################################################
echo '***' Starting word count test.

# Word Count
../mrsequential ../apps/wordcount.jar WordCount ../data/pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

# start multiple workers.
timeout -k 2s 180s ../mrworker ../apps/wordcount.jar WordCount 0 3 &
timeout -k 2s 180s ../mrworker ../apps/wordcount.jar WordCount 1 3 &
timeout -k 2s 180s ../mrworker ../apps/wordcount.jar WordCount 2 3 &

echo '-- wait 2s for workers to start --'
sleep 2

timeout -k 2s 180s ../mrcoordinator 3 ../data/pg*txt &
pid=$!

# wait for the coordinator to exit.
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

wait # for workers and coordinator to exit 

#########################################################
echo '***' Starting map parallelism test.

rm -f mr-*

timeout -k 2s 180s ../mrworker ../apps/map-parallel.jar MapParallel 0 2 &
timeout -k 2s 180s ../mrworker ../apps/map-parallel.jar MapParallel 1 2 &

echo '-- wait 2s for workers to start --'
sleep 2

timeout -k 2s 180s ../mrcoordinator 2 ../data/pg*txt # << block

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

wait # for workers to exit

#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

timeout -k 2s 180s ../mrworker ../apps/reduce-parallel.jar ReduceParallel 0 2 &
timeout -k 2s 180s ../mrworker ../apps/reduce-parallel.jar ReduceParallel 1 2 &

echo '-- wait 2s for workers to start --'
sleep 2

timeout -k 2s 180s ../mrcoordinator 2 ../data/pg*txt # << block

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

# kill possible remaining processes
# kill $(jps | grep worker | awk '{print $1}')
# kill $(jps | grep coordinator | awk '{print $1}')
