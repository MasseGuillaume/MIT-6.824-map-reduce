#!/usr/bin/env bash

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1


# Word Count
../mrsequential ../apps/wordcount.jar ../data/pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*
