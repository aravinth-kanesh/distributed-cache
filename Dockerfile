# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o dcache cmd/server/main.go

# Runtime stage
FROM alpine:3.21

RUN apk --no-cache add ca-certificates
COPY --from=builder /build/dcache /usr/local/bin/dcache

RUN mkdir -p /data
VOLUME /data

EXPOSE 6379 9090

ENTRYPOINT ["dcache"]
CMD ["--dir", "/data", "--metrics-port", "9090"]
