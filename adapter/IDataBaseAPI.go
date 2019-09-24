package adapter

import (
	"database/sql"
	. "github.com/shiyongabc/go-sql-api/types"
)

type IDatabaseAPI interface {
	Create(table string, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage)
	CreateSql(table string, obj map[string]interface{}) (sql string,errorMessage *ErrorMessage)
	ReplaceCreate(table string, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage)
	RelatedCreate(operates []map[string]interface{},  obj map[string]interface{},submitPerson string) (rowAaffect int64,masterKey string,masterId string,errorMessage *ErrorMessage)
	CreateTableStructure(execSql string) (errorMessage *ErrorMessage)
	RelatedUpdate(operates []map[string]interface{}, obj map[string]interface{},submitPerson string) (rowAaffect int64,errorMessage *ErrorMessage)
	Update(table string, id interface{}, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage)
	UpdateSql(table string, id interface{}, obj map[string]interface{}) (sql string,errorMessage *ErrorMessage)
	UpdateBatch(table string, where map[string]WhereOperation, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage)
	UpdateBatchSql(table string, where map[string]WhereOperation, obj map[string]interface{}) (sql string,errorMessage *ErrorMessage)
	Delete(table string, id interface{}, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage)
	DeleteSql(table string, id interface{}, obj map[string]interface{}) (sql string,errorMessage *ErrorMessage)
	Select(option QueryOption) (rs []map[string]interface{},errorMessage *ErrorMessage)
	ExecFunc(sql string) (rs []map[string]interface{},errorMessage *ErrorMessage)
	ExecSql(sql string) (rs []map[string]interface{},errorMessage *ErrorMessage)
	ExecFuncForOne(sql string,key string)(result string,errorMessage *ErrorMessage)

	SelectTotalCount(option QueryOption) (totalCount int,errorMessage *ErrorMessage)
	GetDatabaseMetadata() *DataBaseMetadata
	GetDatabaseMetadataWithView() *DataBaseMetadata
	GetDatabaseTableMetadata(tableName string) *TableMetadata
	UpdateAPIMetadata() (api IDatabaseAPI)
	Connection()*sql.DB
	//RelatedUpdate( obj map[string]interface{}) (rowAaffect int64,errorMessage *ErrorMessage)
}



