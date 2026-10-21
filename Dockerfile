# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o cuota-server .

# Production stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/cuota-server .
COPY config.example.yaml config.yaml

EXPOSE 8318

ENTRYPOINT ["/app/cuota-server", "-c", "/app/config.yaml"]
