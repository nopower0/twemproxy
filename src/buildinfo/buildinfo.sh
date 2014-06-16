#!/bin/bash

# usage: ./version.sh

git_branch=$(git branch --no-color 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/\1/' -e 's/[() ]//g' || echo 'default')
git_commit=$(git rev-parse --short HEAD 2>/dev/null || echo '0000000')
#git_commit=`(git show-ref --head --hash=8 2>/dev/null || echo '0000000') | head -n1`

git_dirty=$(git diff -U0 --no-ext-diff HEAD 2>/dev/null | wc -l)

date=$(date +%Y%m%d_%H%M%S)

echo "${date}_${git_branch}_${git_commit}_dirty_${git_dirty}"
