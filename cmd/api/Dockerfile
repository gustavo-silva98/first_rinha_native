FROM golang:1.24-alpine3.22

WORKDIR /app

COPY ./cmd/api/certifi.crt* /usr/local/share/ca-certificates/certifi.crt

RUN update-ca-certificates

COPY ../go.mod ../go.sum ./
RUN go mod download

COPY ./cmd/api/*.go ./

RUN ls

RUN CGO_ENABLED=0 GOOS=linux go build -o api .

CMD [ "./api" ]

