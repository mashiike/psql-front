FROM golang:1.19-bullseye AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG Version
RUN go build -o psql-front -ldflags "-s -w -X github.com/mashiike/psql-front.Version=${Version}" /app/cmd/psql-front/*.go

FROM debian:bullseye-slim
COPY --from=builder /app/psql-front /usr/local/bin/psql-front
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
EXPOSE 5434 8080
CMD [ "psql-front" ]
