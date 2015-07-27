#!/bin/bash -e
# usage: ./goverall.sh [func|html]
# default: generate cover.out

output_null=""
output_file=".cover.out"
if [[ "$1" != "" ]]; then
	output_null=">/dev/null"
fi

mkdir -p .cover
go list ./... | xargs -I% bash -c 'name="%"; go test % -timeout 60s --coverprofile=.cover/${name//\//_} '$output_null
echo "mode: set" > $output_file
cat .cover/* | grep -v mode >> $output_file
rm -r .cover

if [[ "$1" != "" ]]; then
	go tool cover -$1=$output_file
fi
