#!/usr/bin/env bash

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1

failed_any=0
 
printf "\u001bc"

# #########################################################
# echo '***' Starting word count test.

# # Word Count
# ../mrsequential ../apps/wordcount.jar WordCount ../data/pg*txt || exit 1
# sort mr-out-0 > mr-correct-wc.txt
# rm -f mr-out*

# # start multiple workers.
# timeout -k 2s 180s ../mrworker ../apps/wordcount.jar WordCount 0 3 &
# timeout -k 2s 180s ../mrworker ../apps/wordcount.jar WordCount 1 3 &
# timeout -k 2s 180s ../mrworker ../apps/wordcount.jar WordCount 2 3 &

# echo '-- wait 2s for workers to start --'
# sleep 2

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

# wait # for workers and coordinator to exit 

# #########################################################
# echo '***' Starting indexer.

# # Indexer
# ../mrsequential ../apps/indexer.jar Indexer ../data/pg*txt || exit 1
# sort mr-out-0 > mr-correct-indexer.txt
# rm -f mr-out*

# # start multiple workers.
# timeout -k 2s 180s ../mrworker ../apps/indexer.jar Indexer 0 2 &
# timeout -k 2s 180s ../mrworker ../apps/indexer.jar Indexer 1 2 &

# echo '-- wait 2s for workers to start --'
# sleep 2

# timeout -k 2s 180s ../mrcoordinator 2 ../data/pg*txt &
# pid=$!

# # wait for the coordinator to exit.
# wait $pid

# sort mr-out* | grep . > mr-indexer-all
# if cmp mr-indexer-all mr-correct-indexer.txt
# then
#   echo '---' indexer test: PASS
# else
#   echo '---' indexer output is not the same as mr-correct-indexer.txt
#   echo '---' indexer test: FAIL
#   failed_any=1
# fi

# wait # for workers and coordinator to exit 

# #########################################################
# echo '***' Starting map parallelism test.

# rm -f mr-*

# timeout -k 2s 180s ../mrworker ../apps/map-parallel.jar MapParallel 0 2 &
# timeout -k 2s 180s ../mrworker ../apps/map-parallel.jar MapParallel 1 2 &

# echo '-- wait 2s for workers to start --'
# sleep 2

# timeout -k 2s 180s ../mrcoordinator 2 ../data/pg*txt # << block

# NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
# if [ "$NT" != "2" ]
# then
#   echo '---' saw "$NT" workers rather than 2
#   echo '---' map parallelism test: FAIL
#   failed_any=1
# fi

# if cat mr-out* | grep '^parallel.* 2' > /dev/null
# then
#   echo '---' map parallelism test: PASS
# else
#   echo '---' map workers did not run in parallel
#   echo '---' map parallelism test: FAIL
#   failed_any=1
# fi

# wait # for workers to exit

# #########################################################
# echo '***' Starting reduce parallelism test.

# rm -f mr-*

# timeout -k 2s 180s ../mrworker ../apps/reduce-parallel.jar ReduceParallel 0 2 &
# timeout -k 2s 180s ../mrworker ../apps/reduce-parallel.jar ReduceParallel 1 2 &

# echo '-- wait 2s for workers to start --'
# sleep 2

# timeout -k 2s 180s ../mrcoordinator 2 ../data/pg*txt # << block

# NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
# if [ "$NT" -lt "2" ]
# then
#   echo '---' too few parallel reduces.
#   echo '---' reduce parallelism test: FAIL
#   failed_any=1
# else
#   echo '---' reduce parallelism test: PASS
# fi

# wait

# # Gui: This test does not really make sense
# #########################################################
# echo '***' Starting job count test.

# rm -f mr-*

# timeout -k 2s 180s ../mrworker ../apps/jobcount.jar JobCount 0 2 &
# timeout -k 2s 180s ../mrworker ../apps/jobcount.jar JobCount 1 2 &


# echo '-- wait 2s for workers to start --'
# sleep 2

# timeout -k 2s 180s ../mrcoordinator 2 ../data/pg*txt # << block

# NT=`cat mr-out* | awk '{print $2}'`
# if [ "$NT" -ne "8" ]
# then
#   echo '---' map jobs ran incorrect number of times "($NT != 8)"
#   echo '---' job count test: FAIL
#   failed_any=1
# else
#   echo '---' job count test: PASS
# fi

# wait

# #########################################################
# test whether any worker or coordinator exits before the
# task has completed (i.e., all output files have been finalized)
# rm -f mr-*

# echo '***' Starting early exit test.


# # start multiple workers.
# timeout -k 2s 180s ../mrworker ../apps/early-exit.jar EarlyExit0 0 3 &
# timeout -k 2s 180s ../mrworker ../apps/early-exit.jar EarlyExit0 1 3 &
# timeout -k 2s 180s ../mrworker ../apps/early-exit.jar EarlyExit0 2 3 &

# echo '-- wait 2s for workers to start --'
# sleep 2

# timeout -k 2s 180s ../mrcoordinator 3 ../data/pg*txt &
# pid=$!

# # wait for any of the coord or workers to exit
# # `jobs` ensures that any completed old processes from other tests
# # are not waited upon
# jobs &> /dev/null
# wait -n

# # a process has exited. this means that the output should be finalized
# # otherwise, either a worker or the coordinator exited early
# sort mr-out* | grep . > mr-wc-all-initial

# # wait for remaining workers and coordinator to exit.
# wait

# # compare initial and final outputs
# sort mr-out* | grep . > mr-wc-all-final
# if cmp mr-wc-all-final mr-wc-all-initial
# then
#   echo '---' early exit test: PASS
# else
#   echo '---' output changed after first worker exited
#   echo '---' early exit test: FAIL
#   failed_any=1
# fi
# rm -f mr-*

#########################################################
echo '***' Starting crash test.

# generate the correct output
../mrsequential ../apps/no-crash.jar NoCrash ../data/pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*
rm -f mr-done

# respawn workers until the job is done
loop_workers () {
  ( while [ ! -f mr-done ] 
    do
      echo "Restarting worker: $1"
      timeout -k 2s 180s ../mrworker ../apps/crash.jar Crash "$1" 3
      sleep 1
    done
  ) &
}

# start multiple workers.
loop_workers 0
loop_workers 1
loop_workers 2

echo '-- wait 2s for workers to start --'
sleep 2

(timeout -k 2s 180s ../mrcoordinator 3 ../data/pg*txt ; touch mr-done ) &

wait # for workers to finish

sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi


# # # kill possible remaining processes
# kill $(jps | grep worker | awk '{print $1}')
# kill $(jps | grep coordinator | awk '{print $1}')