#!/bin/bash -e
# usage: ./goverall.sh [func|html]
# default: generate cover.out

output_null=""
output_file=".cover.out"
if [[ "$1" != "" ]]; then
	output_null=">/dev/null"
fi

mkdir -p .cover
go list ./... | xargs -I% bash -c 'name="%"; go test % --coverprofile=.cover/${name//\//_} '$output_null
echo "mode: set" > $output_file
if [[ -d ".cover" ]]; then
	exit 0
fi
cat .cover/* | grep -v mode >> cover.out
rm -r .cover

if [[ "$1" != "" ]]; then
	go tool cover -$1=$output_file
fi
