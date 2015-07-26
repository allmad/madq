install:
	go install ./...

test: install
	go test ./...

cover:
	./goverall.sh
