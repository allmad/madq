#!/bin/bash
if [[ "$1" == "" ]]; then
	echo "usage: $0 <github.com/xxx/xxx>" >&2
	exit 1
fi
git submodule add https://$1 deps/src/$1
