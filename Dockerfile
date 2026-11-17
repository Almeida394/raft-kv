# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/raftkv ./cmd/raftkv

FROM alpine:3.20
RUN adduser -D -u 10001 raftkv
COPY --from=builder /out/raftkv /usr/local/bin/raftkv

USER raftkv
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/raftkv"]
