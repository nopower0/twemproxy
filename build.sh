#!/bin/bash

set -e

if [[ ! -e configure ]]; then
    echo '## configure is not found, autoreconf ... ##'
    autoreconf -fvi
fi

if [[ ! -e Makefile ]]; then
    echo '## Makefile is not found, configure ... ##'
    ./configure --enable-debug=log 
fi

echo '## Makefile is ready, make ... ##'
make
echo 
echo '## make finished ##'

