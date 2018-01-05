FROM golang:1.9-alpine

ADD . .

RUN go install main

EXPOSE 7888

CMD ["main"]