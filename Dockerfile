FROM golang:1.21-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o /bin/accel-proxy   ./cmd/proxy      && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o /bin/mux-server    ./cmd/mux-server  && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o /bin/mux-client    ./cmd/mux-client

FROM scratch
COPY --from=builder /bin/accel-proxy /bin/mux-server /bin/mux-client /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
# Default entrypoint is accel-proxy; override with --entrypoint for mux binaries.
ENTRYPOINT ["/accel-proxy"]
CMD ["-config", "/etc/accel-proxy/config.yaml"]
