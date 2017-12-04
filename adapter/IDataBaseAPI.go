package adapter

import (
	"database/sql"
	. "github.com/shiyongabc/go-mysql-api/types"
)

type IDatabaseAPI interface {
	Create(table string, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage)
	RelatedCreate(templateId string, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage)
	Update(table string, id interface{}, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage)
	Delete(table string, id interface{}, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage)
	Select(option QueryOption) (rs []map[string]interface{},errorMessage *ErrorMessage)
	SelectTotalCount(option QueryOption) (totalCount int,errorMessage *ErrorMessage)
	GetDatabaseMetadata() *DataBaseMetadata
	UpdateAPIMetadata() (api IDatabaseAPI)
}



