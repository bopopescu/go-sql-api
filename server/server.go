package server

import (
	"github.com/shiyongabc/go-mysql-api/server/lib"
	"github.com/labstack/echo"
	"github.com/shiyongabc/go-mysql-api/adapter"
	"github.com/robfig/cron"
)

// MysqlAPIServer is a http server could access mysql api
type MysqlAPIServer struct {
	*echo.Echo               // echo web server
	api adapter.IDatabaseAPI // database api adapter
}

// New create a new MysqlAPIServer instance
func New(api adapter.IDatabaseAPI) *MysqlAPIServer {
	server := &MysqlAPIServer{}
	server.Echo = echo.New()
	server.HTTPErrorHandler = lib.ErrorHandler
	server.HideBanner = true
	server.Logger = lib.Logger
	server.Use(lib.LoggerMiddleware)
	server.Static("/api/docs", "docs")
	server.api = api
	databaseName:=api.GetDatabaseMetadata().DatabaseName
	mountEndpoints(server.Echo, server.api,databaseName)
	return server
}

// Start server
func (server *MysqlAPIServer) Start(address string) *MysqlAPIServer {
	server.StartMetadataRefreshCron()
	server.Logger.Infof("server start at %s", address)
	server.Logger.Fatal(server.Echo.Start(address))
	return server
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
