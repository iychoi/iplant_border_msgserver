#!/bin/bash

export LD_LIBRARY_PATH=.

if [ ! -f "test_lib" ]
then
    echo "test_lib file not found! - build"
    
    # build first
    g++ ../src/test_lib.cpp -L. -lfslib -o ./test_lib
fi

if [ $? == 0 ]
then
    echo "run test_lib"
    ./test_lib $1 $2
fi

