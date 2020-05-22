package main

import (
	"github.com/mkideal/cli"
	"github.com/shiyongabc/go-sql-api/adapter/mysql"
	"github.com/shiyongabc/go-sql-api/server"
	"net/http"

	//"net/http"
    _"net/http/pprof"
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
	RedisPassword      string `cli:"*p,*rpass" usage:"mysql connection str" dft:"$API_REDIS_PASSWORD"`
}

func main() {
	//midStr:=util.GetPhysicalID()
	//mid, _ := strconv.ParseInt(midStr, 10, 64)
	//lib.Logger.Infof("mid=",mid)
	//util.SetMachineId(mid)
	//priId:=util.GetSnowflakeId()
	//lib.Logger.Infof("priId=",priId)// 6620249942380277761  6620250015394721793  6620250111490420736  6620249522821922816  6620249621635530752  6620249679374319616
    // 定时任务	logPrintCron()
	//str:=util.GetBetweenStr("/*ASS_VAR*//*JUDE_SINGLE*/$Stest$E SET maxNo=(SELECT MAX(`stu_no`) AS result FROM test.`stu`);","$S","$E")
	//println("str",str)
	//b:=util.ValidSqlInject("29994a91-aee1-4ff8-be26-f34f3db6e562")
	//println("b",b)
	// 38719
	//re:=convertToFormatDay("38919")
	//print("re=%s",re)
	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:19888", nil))
	}()
	cli.Run(new(cliArgs), func(ctx *cli.Context) error {
		argv := ctx.Argv().(*cliArgs)
		api := mysql.NewMysqlAPI(argv.ConnectionStr, !argv.NoInfomationSchema)
		redisHost:=argv.RedisHost
		redisPassword:=argv.RedisPassword
		server.New(api,redisHost,redisPassword).Start(argv.ListenAddress)

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
