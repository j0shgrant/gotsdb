# build image
FROM golang:1.17-alpine3.15 as build

WORKDIR /build
COPY . .
RUN go build -o . .

# runtime image
FROM alpine:3.15

WORKDIR /opt/gotsdb
RUN addgroup -S gotsdb && adduser -S gotsdb -G gotsdb && \
    chown gotsdb:gotsdb /opt/gotsdb
USER gotsdb
COPY --from=build /build/gotsdb .

CMD [ "./gotsdb" ]
