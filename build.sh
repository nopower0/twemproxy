#!/bin/bash

set -e

# parameters

if [[ ! -z "$1" && "$1" == "debug" ]]; then
    is_expecting_debug=1
else
    is_expecting_debug=0
fi

# configure

if [[ ! -e configure ]]; then
    echo
    echo '## configure is not found, autoreconf ... ##'
    autoreconf -fvi
fi

# makefile

if [[ -e Makefile ]]; then
    set +e
    is_actual_debug=$(cat Makefile | grep 'CFLAGS' | grep -v '\-O' -c)
    set -e
    echo
    echo '## debug options checking ##'
    echo "actual debug $is_actual_debug, expecting debug $is_expecting_debug"
    if [[ $is_expecting_debug != $is_actual_debug ]]; then
        rm -f Makefile
    fi
fi

if [[ ! -e Makefile ]]; then
    echo
    echo '## Makefile is not found, configure ... ##'
    if [[ $is_expecting_debug ]]; then
        CFLAGS=-g CXXFLAGS=-g ./configure --enable-debug=full
    else
        ./configure --enable-debug=log
    fi
fi

echo
echo '## Makefile is ready, make ... ##'
make
echo 
echo '## make finished ##'

