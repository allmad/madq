all:
	go install ./...

test:
	go test -v ./...

cover:
	@../scripts/goverall.sh

show-cover:
	@../scripts/goverall.sh html
