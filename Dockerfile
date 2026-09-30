FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/shard ./cmd/shard

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=build /out/shard /usr/local/bin/shard
COPY configs ./configs

EXPOSE 8080 9090

ENTRYPOINT ["/usr/local/bin/shard"]
