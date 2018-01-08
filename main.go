package main

import (
	"github.com/shiyongabc/go-mysql-api/server"
	"github.com/mkideal/cli"
	"github.com/shiyongabc/go-mysql-api/adapter/mysql"

	"strings"
)

type cliArgs struct {
	cli.Helper
	ConnectionStr      string `cli:"*c,*conn" usage:"mysql connection str" dft:"$API_CONN_STR"`
	ListenAddress      string `cli:"*l,*listen" usage:"listen host and port" dft:"$API_HOST_LS"`
	NoInfomationSchema bool `cli:"n,noinfo" usage:"dont use mysql information shcema" dft:"$API_NO_USE_INFO"`
}

func main() {
	cli.Run(new(cliArgs), func(ctx *cli.Context) error {
		argv := ctx.Argv().(*cliArgs)
		api := mysql.NewMysqlAPI(argv.ConnectionStr, !argv.NoInfomationSchema)
		hostStrArr:=[]string{}
		hostStrArr=strings.Split(argv.ConnectionStr,"(")
		//fmt.Printf("host",hostStrArr)
		endIndex:=strings.LastIndex(hostStrArr[1],":")
		redisHost:=string(hostStrArr[1][0:endIndex])
//fmt.Printf("host=",string(hostStrArr[1][0:endIndex]))
		server.New(api,redisHost).Start(argv.ListenAddress)
		return nil
	})

}
