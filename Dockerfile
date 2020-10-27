FROM    golang:1.14 as builder
COPY    ./     /buildDir

WORKDIR /buildDir
RUN     ./goBuild.sh docker

FROM    alpine:3.12
COPY    ./sample-config.yaml /app/config.yaml
COPY    ./test-input.txt /app/test-input.txt
COPY    --from=builder /buildDir/cmd/bin/process.bin /app/process

WORKDIR /app

ENTRYPOINT  [ "/app/process" ]
CMD         [ "--config", "/app/config.yaml" ]
