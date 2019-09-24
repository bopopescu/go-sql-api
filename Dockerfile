FROM alpine
ENV TZ=Asia/Shanghai

ENV API_HOST_LS :80
RUN apk add --no-cache tzdata && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
COPY go-mysql-api-linux-amd64 /go-sql-api
COPY docs /docs
COPY upload /upload
RUN chmod +x /go-sql-api
EXPOSE 80

CMD ["/go-sql-api"]
