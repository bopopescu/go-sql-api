package lib

import (
	"os"
	. "github.com/xuybin/go-mysql-api/types"
	"github.com/labstack/gommon/log"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"reflect"
	"net/http"
)

// Logger public Logger
var Logger = newLogger()

// NewLogger for instance
func newLogger() (l *log.Logger) {
	l = log.New("database-api-server")
	l.SetHeader(`[${level}] ${time_rfc3339_nano}`)
	l.SetLevel(log.DEBUG)
	l.SetOutput(os.Stdout)
	return
}
var LoggerMiddleware=middleware.LoggerWithConfig(middleware.LoggerConfig{
	Format: "[REQ] ${time_rfc3339_nano} ${method} (HTTP${status}) ${uri} ${latency}ns\n",
})

var ErrorHandler= func (err error, c echo.Context) {
	if reflect.TypeOf(err) == reflect.TypeOf(&echo.HTTPError{}) {
		httpError := err.(*echo.HTTPError)
		c.JSON(httpError.Code, httpError.Message)
	}else if reflect.TypeOf(err) == reflect.TypeOf(&ErrorMessage{}) {
		errorMessage := err.(*ErrorMessage)
		c.JSON(http.StatusInternalServerError, errorMessage)
	} else {
		c.JSON(http.StatusInternalServerError, &ErrorMessage{"unknown_error", err.Error()})
	}
}