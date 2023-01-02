#!/bin/bash

lb create --overwrite test

echo "**** prov MV ****"

echo "==== prov_exec ===="
time lb addblock test -f prov_exec.logic

# echo "==== Query: answer ===="
# time lb query test "
#     _(n,c) <- path_asr_count(n,c).
# " 
