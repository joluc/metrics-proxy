FROM golang:1.26-bookworm AS builder

WORKDIR /src
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/metrics-proxy ./cmd/metrics-proxy

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/metrics-proxy /usr/local/bin/metrics-proxy

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/metrics-proxy"]
