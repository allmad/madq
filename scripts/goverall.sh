#!/bin/bash
set -e

# usage: ./goverall.sh [func|html]
# default: generate cover.out
output_null=""
output_file=".cover.out"

list_cmd="go list ./... | grep -v muxque"
if [[ "$2" != "" ]]; then
	list_cmd="echo '$2'"
fi

mkdir -p .cover
eval "$list_cmd" | xargs -I% bash -c 'name="%"; go test % --coverprofile=.cover/${name//\//_}'$output_null
echo "mode: set" > $output_file
cat .cover/* | grep -v mode >> $output_file
rm -r .cover

if [[ "$1" != "" ]]; then
	go tool cover -$1=$output_file
fi
