FROM            telegraf:alpine

COPY ./etconfig.yaml /app/config.yaml
COPY ./bin/process.bin /app/process

WORKDIR /app

ENTRYPOINT [ "/app/process" ]
CMD [ "--config", "/app/config.yaml" ]
