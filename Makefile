.DEFAULT_GOAL := run
.PHONY: run build test lint tidy

run:
	go run ./cmd/app

build:
	go build -o ./bin/app ./cmd/app

test:
	go test ./... -race -count=1

lint:
	go vet ./...
	golangci-lint run ./...

tidy:
	go mod tidy