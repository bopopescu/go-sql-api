package server

import (
	"github.com/labstack/echo"
	"github.com/robfig/cron"
	"github.com/shiyongabc/go-sql-api/adapter"
	"github.com/shiyongabc/go-sql-api/server/lib"
)

// MysqlAPIServer is a http server could access mysql api
type MysqlAPIServer struct {
	*echo.Echo               // echo web server
	api adapter.IDatabaseAPI // database api adapter
}

// New create a new MysqlAPIServer instance
func New(api adapter.IDatabaseAPI,redisHost string,redisPassword string) *MysqlAPIServer {
	server := &MysqlAPIServer{}
	server.Echo = echo.New()
	server.HTTPErrorHandler = lib.ErrorHandler
	server.HideBanner = true
	server.Logger = lib.Logger
	server.Use(lib.LoggerMiddleware)
	databaseName:=api.GetDatabaseMetadataWithView().DatabaseName
	server.Static("/api/"+databaseName+"/docs", "docs")
	server.api = api
//	databaseName:=api.GetDatabaseMetadata().DatabaseName
  lib.Logger.Infof("redisHost=%s",redisHost)
	//c, err := redis.Dial("tcp", redisHost)
	//if err != nil {
	//	fmt.Println("Connect to redis error", err)
	//	c=nil
	//}else{
	//	fmt.Println("Connect to redis success")
	//}

	mountEndpoints(server.Echo, server.api,databaseName,redisHost,redisPassword)
	return server
}

// Start server
func (server *MysqlAPIServer) Start(address string) *MysqlAPIServer {
	server.StartMetadata()
	server.Logger.Infof("server start at %s", address)
	server.Logger.Fatal(server.Echo.Start(address))
	return server
}
func (m *MysqlAPIServer) StartMetadata() {
	m.api.UpdateAPIMetadata()
	m.Logger.Infof("metadata updated !")

}
// StartMetadataRefreshCron task
func (m *MysqlAPIServer) StartMetadataRefreshCron() {
		c := cron.New()
		c.AddFunc("@every 5m", func() {
			m.api.UpdateAPIMetadata()
			m.Logger.Infof("metadata updated !")
		})
		c.Start()
}
