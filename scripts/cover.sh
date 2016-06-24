#!/bin/bash
go test -covermode count -coverprofile=/tmp/a.out && go tool cover -html=/tmp/a.out
