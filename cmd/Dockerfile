FROM golang:1.24-alpine3.22

WORKDIR /app

COPY ../go.mod ../go.sum ./
RUN go mod download

COPY ./cmd/*.go ./

RUN ls

RUN CGO_ENABLED=0 GOOS=linux go build -o app .

CMD [ "./app" ]

