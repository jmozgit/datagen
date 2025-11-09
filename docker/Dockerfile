FROM golang:1.24-alpine AS builder

ARG OS
ARG ARCH

WORKDIR /app

COPY . .

RUN GOOS=${OS} GOARCH=${ARCH} go build -o datagen ./cmd/datagen/main.go

