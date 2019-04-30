FROM alpine
ENV TZ=Asia/Shanghai

ENV API_HOST_LS :80
RUN apk add tzdata && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
COPY go-mysql-api-linux-amd64 /go-mysql-api
COPY docs /docs
COPY upload /upload
RUN chmod +x /go-mysql-api
EXPOSE 80

CMD ["/go-mysql-api"]
