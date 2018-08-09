FROM golang:1.10.3-alpine3.8

RUN apk add git
RUN go get github.com/sundy-li/burrowx && \
  cd $GOPATH/src/github.com/sundy-li/burrowx && \
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build


FROM alpine:3.8
WORKDIR /app
COPY --from=0 /go/src/github.com/sundy-li/burrowx/burrowx .
COPY server.json logging.xml ./
CMD ["/app/burrowx", "--config", "server.json"] 
