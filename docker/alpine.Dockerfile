FROM alpine:3.8

RUN apk add --update --no-cache clang-dev git openssl-dev

RUN apk add --no-cache --repository http://dl-cdn.alpinelinux.org/alpine/edge/community cargo

RUN git clone https://github.com/graphprotocol/graph-node &&\
    cd graph-node &&\
    git checkout master

RUN cargo install --path node --root /usr/local &&\
    cd .. &&\
    rm -rf graph-node &&\
    apk del cargo

ENTRYPOINT ["graph-node"]
