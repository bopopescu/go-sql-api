package main

import (
	"github.com/shiyongabc/go-sql-api/server"
	"github.com/mkideal/cli"
	"github.com/shiyongabc/go-sql-api/adapter/mysql"

//	"fmt"
	"github.com/robfig/cron"
	"log"
)

type cliArgs struct {
	cli.Helper
	ConnectionStr      string `cli:"*c,*conn" usage:"mysql connection str" dft:"$API_CONN_STR"`
	ListenAddress      string `cli:"*l,*listen" usage:"listen host and port" dft:"$API_HOST_LS"`
	NoInfomationSchema bool `cli:"n,noinfo" usage:"dont use mysql information shcema" dft:"$API_NO_USE_INFO"`
	RedisHost      string `cli:"*r,*rhost" usage:"mysql connection str" dft:"$API_REDIS_HOST"`
}

func main() {


    // 定时任务	logPrintCron()
	cli.Run(new(cliArgs), func(ctx *cli.Context) error {
		argv := ctx.Argv().(*cliArgs)
		api := mysql.NewMysqlAPI(argv.ConnectionStr, !argv.NoInfomationSchema)
		redisHost:=argv.RedisHost
		server.New(api,redisHost).Start(argv.ListenAddress)
		return nil
	})



}
func logPrintCron(){
	i := 0
	c := cron.New()
	spec := "*/5 * * * * ?"
	c.AddFunc(spec, func() {
		i++
		log.Println("cron running:", i)
	})
	c.AddFunc("@every 1h1m", func() {
		i++
		log.Println("cron running:", i)
	})
	c.Start()

}