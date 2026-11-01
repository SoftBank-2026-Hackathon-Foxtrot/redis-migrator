FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY internal ./internal
COPY cmd ./cmd
RUN CGO_ENABLED=0 go build -o /out/statemig-proxy ./cmd/proxy && \
    CGO_ENABLED=0 go build -o /out/statemig-syncworker ./cmd/syncworker && \
    CGO_ENABLED=0 go build -o /out/statemig-latency ./cmd/latency

FROM alpine:3.20
COPY --from=build /out/ /usr/local/bin/
EXPOSE 6379 7000 7100
ENTRYPOINT ["statemig-proxy"]
