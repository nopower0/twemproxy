#!/bin/bash

set -e

if [[ ! -e Makefile ]]; then
    autoreconf -fvi && ./configure --enable-debug=log 
fi

make
echo 
echo "make finished"

