#!/bin/bash

function parse_args() {
    for argument in "$@"
    do
        echo "${argument}"
        local key=$(echo ${argument} | cut -f1 -d=)
        local key_length=${#key}
        local value="${argument:$key_length+1}"
        export "$key"="$value"
    done
}

function validate_args() {
    for key in OS ARCH BUILDMODE GO_BUILD_PATH COPY_CONTEXT BIN_DST
    do
        if [ -z ${!key} ]; then
            echo "$key is not set"
            exit 1
        fi
    done
}

function build_docker_image() {
    cat << 'EOF' | docker build \
               --build-arg OS=${OS} \
               --build-arg ARCH=${ARCH} \
               --build-arg BUILDMODE=${BUILDMODE} \
               --build-arg GO_BUILD_PATH=${GO_BUILD_PATH} \
               --build-arg COPY_CONTEXT=${COPY_CONTEXT} \
               -t tmpimg \
               -f - ${COPY_CONTEXT}
    FROM golang:1.24-trixie AS builder

    ARG OS
    ARG ARCH
    ARG BUILDMODE
    ARG NAME
    ARG GO_BUILD_PATH
    ARG COPY_CONTEXT

    WORKDIR /app

    COPY ${COPY_CONTEXT} .

    RUN CGO_ENABLED=1 GOOS=${OS} GOARCH=${ARCH} go build -buildmode=${BUILDMODE} -o out ${GO_BUILD_PATH}  
EOF
    return $?
}

function build() {
    local id="$(docker create tmpimg)"
    docker cp ${id}:/app/out ${BIN_DST}
    docker rm ${id}
}

parse_args $@
validate_args
build_docker_image
local ret=$?
if [ $ret -ne 0 ]; then
    echo "Failed to build go binary"
    exit 1
else
    build
fi
