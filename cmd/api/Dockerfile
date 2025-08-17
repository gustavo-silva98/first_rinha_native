FROM golang:1.24-alpine3.22

WORKDIR /app

COPY ../go.mod ../go.sum ./
RUN go mod download

COPY ./cmd/api/*.go ./
COPY ./internal/ ./internal/


RUN ls

RUN CGO_ENABLED=0 GOOS=linux go build -o api .

CMD [ "./api" ]

