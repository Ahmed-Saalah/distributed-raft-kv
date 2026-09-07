FROM golang:alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o kvserver ./cmd/kvserver

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/kvserver .

EXPOSE 5000

ENTRYPOINT ["./kvserver"]