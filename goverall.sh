#!/bin/bash -e
# usage: ./goverall.sh [func|html]
# default: generate cover.out

#!/bin/bash
set -e

# usage: ./goverall.sh [func|html]
# default: generate cover.out
output_null=""
if [[ "$1" != "" ]]; then
	output_null=">/dev/null"
fi

mkdir -p .cover
go list ./... | xargs -I% bash -c 'name="%"; go test % --coverprofile=.cover/${name//\//_} '$output_null
echo "mode: set" > cover.out
cat .cover/* | grep -v mode >> cover.out
rm -r .cover

if [[ "$1" != "" ]]; then
	go tool cover -$1=cover.out
fi
