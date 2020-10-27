#!/bin/bash

BT=$(date +"%Y.%m.%d.%H%M%S")
CH=$(git rev-list -1 HEAD)
if [ "${1}" == "local" ]; then
    cd cmd && \
    GOOS=linux ARCH=amd64 go build -ldflags "-X main.buildTime=${BT} -X main.commitHash=${CH}" -o cmd/bin/process.bin ./
fi

if [ "${1}" == "docker" ]; then
    cd /buildDir/cmd && \
    go mod tidy && \
    CGO_ENABLED=0 GOOS=linux ARCH=amd64 go build -a -ldflags "-X main.buildTime=${BT} -X main.commitHash=${CH} -s -w -extldflags '-static'" -o bin/process.bin ./
fi
