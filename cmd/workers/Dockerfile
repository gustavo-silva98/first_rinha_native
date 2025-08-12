FROM golang:1.24-alpine3.22 AS builder

WORKDIR /app

COPY ../go.mod ../go.sum ./
RUN go mod download

COPY ./cmd/workers/*.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o worker .

FROM scratch

WORKDIR /app

COPY --from=builder /app/worker ./

CMD [ "./worker" ]

