FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/shard ./cmd/shard
RUN CGO_ENABLED=0 go build -trimpath -o /out/traffic-gen ./cmd/traffic-gen

# Local-only traffic generator image (see docker-compose.yml's traffic-gen
# service). Not part of the default build target, so `docker build .` with
# no --target still produces the shard image below, unchanged.
FROM alpine:3.20 AS traffic-gen

RUN apk add --no-cache ca-certificates
COPY --from=build /out/traffic-gen /usr/local/bin/traffic-gen

ENTRYPOINT ["/usr/local/bin/traffic-gen"]

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=build /out/shard /usr/local/bin/shard
COPY configs ./configs

EXPOSE 8080 9090

ENTRYPOINT ["/usr/local/bin/shard"]
