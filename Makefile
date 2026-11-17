.PHONY: build test testrace bench run

build:
	go build -o bin/raftkv ./cmd/raftkv

test:
	go test ./...

testrace:
	go test -race ./...

bench:
	go test -run=^$$ -bench=. ./test/bench/...

run: build
	./bin/raftkv
