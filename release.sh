#!/bin/bash

set -e

./build.sh

bin_file=twemproxy
if [[ ! -z "$1" && "$1" == "abtest" ]]; then
    bin_file=twemproxy-abtest
fi
output_dir=../twemproxy_bin/bin
if [[ -d $output_dir && -w $output_dir ]]; then
    cp -rvp ./src/nutcracker $output_dir/$bin_file
    echo "cp bin finished"
else
    echo "$output_dir not exists or access denied"
fi
