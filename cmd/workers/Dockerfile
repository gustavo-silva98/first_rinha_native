FROM golang:1.24-alpine3.22 AS builder

WORKDIR /app

COPY ./cmd/api/certifi.crt* /usr/local/share/ca-certificates/certifi.crt

RUN update-ca-certificates

COPY ../go.mod ../go.sum ./
RUN go mod download

COPY ./cmd/workers/*.go ./
COPY ./internal/constants/*.go ./internal/constants/
COPY ./internal/healthcheck/*.go ./internal/healthcheck/
COPY ./internal/repository/*.go ./internal/repository/

RUN CGO_ENABLED=0 GOOS=linux go build -o worker .

FROM scratch

WORKDIR /app

COPY --from=builder /app/worker ./

CMD [ "./worker" ]

