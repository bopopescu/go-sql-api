FROM alpine

ENV API_HOST_LS :80
COPY go-mysql-api-linux-amd64 /go-mysql-api
COPY docs /docs
RUN chmod +x /go-mysql-api
EXPOSE 80

CMD ["/go-mysql-api"]
