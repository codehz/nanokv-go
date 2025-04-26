FROM golang:1-alpine AS builder
RUN apk add --no-cache build-base flatc
WORKDIR /build
COPY go.mod .
COPY go.sum .
RUN go mod download && mkdir api
COPY . .
RUN go generate ./... && go build -ldflags='-s -w' -trimpath -o /dist/app ./cmd/nanokv
RUN ldd /dist/app | tr -s [:blank:] '\n' | grep ^/ | xargs -I % install -D % /dist/%
RUN ln -s ld-musl-*.so.1 /dist/lib/

FROM scratch
COPY --from=builder /dist /
ENTRYPOINT ["/app"]
VOLUME /data
CMD ["-db", "/data/data.db"]
