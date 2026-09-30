.DEFAULT_GOAL := run
.PHONY: run build test lint tidy

run:
	go run ./cmd/shard

build:
	go build -o ./bin/shard ./cmd/shard

test:
	go test ./... -race -count=1

lint:
	go vet ./...
	golangci-lint run ./...

tidy:
	go mod tidy