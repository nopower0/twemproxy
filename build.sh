#!/bin/bash

set -e

if [[ ! -e Makefile ]]; then
    autoreconf -fvi && ./configure --enable-debug=log 
fi

make
echo 
echo "make finished"

output_bin=../twemproxy_bin/bin
if [[ -d $output_bin && -w $output_bin ]]; then
    cp -rp ./src/nutcracker $output_bin/twemproxy
    echo "cp bin finished"
else
    echo "$output_bin not exists or access denied"
fi
