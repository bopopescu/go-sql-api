package swagger

import (
	"fmt"
	"testing"

	"os"

	"github.com/shiyongabc/go-mysql-api/adapter/mysql"
)

var connectionStr = os.Getenv("API_CONN_STR")

func TestGenerateSwaggerConfig(t *testing.T) {
	api := mysql.NewMysqlAPI(connectionStr, true)
	defer api.Stop()
	s := GenSwaggerFromDBMetadata(api.GetDatabaseMetadata())
	j, _ := s.MarshalJSON()
	fmt.Println(string(j))
}
