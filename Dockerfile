FROM golang:1.18-bullseye AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG Version
RUN go build -o psql-front -ldflags "-s -w -X github.com/mashiike/psql-front.Version=${Version}" /app/cmd/psql-front/main.go

FROM debian:bullseye-slim
COPY --from=builder /app/psql-front /usr/local/bin/psql-front
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
EXPOSE 5434
CMD [ "psql-front" ]
