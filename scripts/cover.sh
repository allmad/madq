#!/bin/bash
go test -coverprofile=/tmp/a.out && go tool cover -html=/tmp/a.out
