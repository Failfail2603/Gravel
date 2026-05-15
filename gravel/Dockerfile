FROM golang:1.24.4-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/gravel .

FROM alpine:3.20

RUN addgroup -S gravel && adduser -S gravel -G gravel
WORKDIR /app

COPY --from=builder /out/gravel /app/gravel

ENV NATS_HOST=0.0.0.0
ENV NATS_PORT=4222
ENV NATS_CONNECT_HOST=127.0.0.1

EXPOSE 4222

USER gravel

CMD ["/app/gravel"]
