#!/bin/bash

go build -o sort.exe cmd/sort/main.go
go build -o gen.exe cmd/gen/main.go
go build -o check.exe cmd/check/main.go
go build -o test.exe cmd/test/main.go
