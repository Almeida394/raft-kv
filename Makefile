.PHONY: build test testrace bench run

build:
	go build -o bin/raftkv ./cmd/raftkv

test:
	go test ./...

testrace:
	go test -race ./...

bench:
	go test -run=^$$ -bench=. -benchtime=1000x -timeout=5m ./test/bench/...

run: build
	./bin/raftkv
