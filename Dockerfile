FROM golang:1.21-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o /bin/accel-proxy ./cmd/proxy

FROM scratch
COPY --from=builder /bin/accel-proxy /accel-proxy
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["/accel-proxy"]
CMD ["-config", "/etc/accel-proxy/config.yaml"]
