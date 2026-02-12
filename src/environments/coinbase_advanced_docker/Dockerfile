FROM alpine:latest
RUN apk update && apk add --no-cache bash build-base python3 python3-dev && \
    rm /usr/lib/python*/EXTERNALLY-MANAGED && \
    apk add --no-cache py3-pip && \
    pip3 install coinbase-advanced-py rp2 backports.datetime_fromisoformat progressbar2 ccxt
ENTRYPOINT ["/bin/bash"]
