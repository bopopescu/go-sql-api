package mysql

import (
	"database/sql"
	"fmt"
	"time"
	. "github.com/shiyongabc/go-mysql-api/types"
	"github.com/shiyongabc/go-mysql-api/server/lib"
	_ "github.com/go-sql-driver/mysql"
	"github.com/labstack/gommon/log"
	"gopkg.in/doug-martin/goqu.v4"
	_ "gopkg.in/doug-martin/goqu.v4/adapters/mysql"
	"github.com/shiyongabc/go-mysql-api/adapter"
	"strconv"
	"strings"
)

// MysqlAPI
type MysqlAPI struct {
	connection           *sql.DB           // mysql connection
	databaseMetadata     *DataBaseMetadata // database metadata
	sql                  *SQL              // goqu sql builder
	useInformationSchema bool
}

// NewMysqlAPI create new MysqlAPI instance
func NewMysqlAPI(dbURI string, useInformationSchema bool) (api *MysqlAPI) {
	api = &MysqlAPI{}
	err:=createDatabase(api,dbURI)
	if err!=nil{
		panic(err)
	}else {
		api.GetConnectionPool(dbURI)
		api.useInformationSchema = useInformationSchema
		lib.Logger.Infof("connected to mysql with conn_str: %s", dbURI)
		api.UpdateAPIMetadata()
		lib.Logger.Infof("retrived metadata from mysql database: %s", api.databaseMetadata.DatabaseName)
		api.sql = &SQL{goqu.New("mysql", api.connection), api.databaseMetadata}
		return
	}
}

func  createDatabase(api *MysqlAPI,dbURI string) (err error) {
	result:=strings.LastIndex(dbURI,"/")
	if result >= 0 && result+1<len(dbURI){
		dbName:=string([]byte(dbURI)[result+1:len(dbURI)])
		dbURI= string([]byte(dbURI)[0:result+1])
		api.GetConnectionPool(dbURI)
		_, err = api.connection.Exec("CREATE DATABASE IF NOT EXISTS "+dbName)
		api.connection.Close()
		api.connection=nil
	}else {
		err=fmt.Errorf("dataSourceName:%s doesn't exist dbName",dbURI)
	}
	return
}
// Connection return
func (api *MysqlAPI) Connection() *sql.DB {
	return api.connection
}

// SQL instance
func (api *MysqlAPI) SQL() *SQL {
	return api.sql
}

// GetDatabaseMetadata return database meta
func (api *MysqlAPI) GetDatabaseMetadata() *DataBaseMetadata {
	return api.databaseMetadata
}

// UpdateAPIMetadata use to update the metadata of the MySQLAPI instance
//
// If database tables structure changed, it will be useful
func (api *MysqlAPI) UpdateAPIMetadata() adapter.IDatabaseAPI {
	if api.useInformationSchema {
		api.databaseMetadata = api.retriveDatabaseMetadataFromInfoSchema(api.CurrentDatabaseName())
	} else {
		api.databaseMetadata = api.retriveDatabaseMetadata(api.CurrentDatabaseName())
	}
	return api
}

// GetConnectionPool which Pool is Singleton Connection Pool
func (api *MysqlAPI) GetConnectionPool(dbURI string) *sql.DB {
	if api.connection == nil {
		pool, err := sql.Open("mysql", dbURI)
		if err != nil {
			log.Fatal(err.Error())
		}
		// 3 minutes unused connections will be closed
		pool.SetConnMaxLifetime(3 * time.Minute)
		pool.SetMaxIdleConns(3)
		pool.SetMaxOpenConns(10)
		api.connection = pool
	}
	return api.connection
}

// Stop MysqlAPI, clean connections
func (api *MysqlAPI) Stop() *MysqlAPI {
	if api.connection != nil {
		api.connection.Close()
	}
	return api
}

// CurrentDatabaseName return current database
func (api *MysqlAPI) CurrentDatabaseName() string {
	rows, err := api.connection.Query("select database()")
	if err != nil {
		log.Fatal(err)
	}
	var res string
	for rows.Next() {
		if err := rows.Scan(&res); err != nil {
			log.Fatal(err)
		}
	}
	return res
}

func (api *MysqlAPI) retriveDatabaseMetadata(databaseName string) *DataBaseMetadata {
	var tableMetas []*TableMetadata
	rs := &DataBaseMetadata{DatabaseName: databaseName}
	rows, err := api.connection.Query("show tables")
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			log.Fatal(err)
		}
		tableMetas = append(tableMetas, api.retriveTableMetadata(tableName))
	}
	rs.Tables = tableMetas
	return rs
}

func (api *MysqlAPI) retriveDatabaseMetadataFromInfoSchema(databaseName string) (rs *DataBaseMetadata) {
	var tableMetas []*TableMetadata
	rs = &DataBaseMetadata{DatabaseName: databaseName}
	rows, err := api.connection.Query(fmt.Sprintf("SELECT `TABLE_NAME`,`TABLE_TYPE`,`TABLE_ROWS`,`AUTO_INCREMENT`,`TABLE_COMMENT` FROM `information_schema`.`tables` WHERE `TABLE_SCHEMA` = '%s'", databaseName))
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var tableName, tableType, tableComments sql.NullString
		var tableRows, tableIncre sql.NullInt64
		err := rows.Scan(&tableName, &tableType, &tableRows, &tableIncre, &tableComments)
		if err != nil {
			log.Fatal(err)
		}
		tableMeta := &TableMetadata{}
		tableMeta.TableName = tableName.String
		tableMeta.Columns = api.retriveTableColumnsMetadataFromInfoSchema(databaseName, tableName.String)
		tableMeta.Comment = tableComments.String
		tableMeta.TableType = tableType.String
		tableMeta.CurrentIncre = tableIncre.Int64
		tableMeta.TableRows = tableRows.Int64
		tableMetas = append(tableMetas, tableMeta)
	}
	rs.Tables = tableMetas
	return rs
}

func (api *MysqlAPI) retriveTableMetadata(tableName string) *TableMetadata {
	rs := &TableMetadata{TableName: tableName}
	var columnMetas []*ColumnMetadata
	rows, err := api.connection.Query(fmt.Sprintf("desc %s", tableName))
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var columnName, columnType, nullAble, key, defaultValue, extra sql.NullString
		err := rows.Scan(&columnName, &columnType, &nullAble, &key, &defaultValue, &extra)
		if err != nil {
			log.Fatal(err)
		}
		columnMeta := &ColumnMetadata{ColumnName: columnName.String, ColumnType: columnType.String, NullAble: nullAble.String, Key: key.String, DefaultValue: defaultValue.String, Extra: extra.String}
		columnMetas = append(columnMetas, columnMeta)
	}
	rs.Columns = columnMetas
	return rs
}

func (api *MysqlAPI) retriveTableColumnsMetadataFromInfoSchema(databaseName, tableName string) (columnMetas []*ColumnMetadata) {
	rows, err := api.connection.Query(fmt.Sprintf("SELECT `COLUMN_NAME`, `COLUMN_TYPE`,`IS_NULLABLE`,`COLUMN_KEY`,`COLUMN_DEFAULT`,`EXTRA`, `ORDINAL_POSITION`,`DATA_TYPE`,`COLUMN_COMMENT` FROM `Information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s'", databaseName, tableName))
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY, EXTRA, COLUMN_COMMENT sql.NullString
		var ORDINAL_POSITION sql.NullInt64
		err := rows.Scan(&COLUMN_NAME, &COLUMN_TYPE, &IS_NULLABLE, &COLUMN_KEY, &COLUMN_DEFAULT, &EXTRA, &ORDINAL_POSITION, &DATA_TYPE, &COLUMN_COMMENT)
		if err != nil {
			log.Fatal(err)
		}

		columnMeta := &ColumnMetadata{
			COLUMN_NAME.String,
			COLUMN_TYPE.String,
			IS_NULLABLE.String,
			COLUMN_KEY.String,
			COLUMN_DEFAULT.String,
			EXTRA.String,
			ORDINAL_POSITION.Int64,
			DATA_TYPE.String,
			COLUMN_COMMENT.String,
		}
		columnMetas = append(columnMetas, columnMeta)
	}
	return
}

// Query by sql
func (api *MysqlAPI) query(sql string, args ...interface{}) (rs []map[string]interface{}, errorMessage *ErrorMessage) {
	lib.Logger.Debugf("query sql: '%s'", sql)
	rows, err := api.connection.Query(sql, args...)
	if err != nil {
		errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
		return
	}
	// mysql driver not implement rows.ColumnTypes
	cols, _ := rows.Columns()
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			errorMessage= &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
			return
		}
		m := make(map[string]interface{})
		for i, colName := range cols {
			// Yap! Any integer based types will use int types
			// Other types will convert to string, include decimal, date and others
			colV := *columnPointers[i].(*interface{})
			switch (colV).(type) {
			case int64:
				colV = colV.(int64)
			case []uint8:
				colV = fmt.Sprintf("%s", colV)
			}
			m[colName] = colV
		}
		rs = append(rs, m)
	}
	return
}

// Exec a sql
func (api *MysqlAPI) exec(sql string, args ...interface{}) (rs sql.Result,errorMessage *ErrorMessage) {
	lib.Logger.Debugf("exec sql: '%s'", sql)
	rs,err:= api.connection.Exec(sql, args...)
	if err != nil {
		errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
	}
	return
}

// Create by Table name and obj map
func (api *MysqlAPI) Create(table string, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage) {
	sql, err := api.sql.InsertByTable(table, obj)
	if err != nil {
		errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
	}
	return api.exec(sql)
}
// batch Create related table by Table name and obj map
func (api *MysqlAPI) RelatedCreate(templateId string, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage) {


	sql, err := api.sql.InsertByTable(templateId, obj)
	if err != nil {
		errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
	}
	sql="insert into account_db.subject_config(`subject_len`,`first_subject`,`second_subject_len`,`third_subject_len`)values('12','qw','21','32')"
	return api.exec(sql)
}

// Update by Table name and obj map
func (api *MysqlAPI) Update(table string, id interface{}, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage) {
	if id != nil {
		sql, err := api.sql.UpdateByTableAndId(table, id, obj)
		if err != nil {
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
			return
		}
		return api.exec(sql)
	} else {
		errorMessage = &ErrorMessage{ERR_PARAMETER,"Only primary key updates are supported(must primary id) !"}
		return
	}
}

// Delete by Table name and where obj
func (api *MysqlAPI) Delete(table string, id interface{}, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage) {
	var sSQL string
	var err error
	if id != nil {
		sSQL, err= api.sql.DeleteByTableAndId(table, id)
	} else {
		sSQL, err= api.sql.DeleteByTable(table, obj)
	}
	if err != nil {
		errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
		return
	}
	return api.exec(sSQL)
}

// Select by Table name , where or id
func (api *MysqlAPI) Select(option QueryOption) (rs []map[string]interface{},errorMessage *ErrorMessage) {
	var sql string
	var err error
	for _, f := range option.Fields {
		if !api.databaseMetadata.TableHaveField(option.Table, f) {
			errorMessage = &ErrorMessage{ERR_PARAMETER,fmt.Sprintf("Table '%s' not have '%s' field !", option.Table, f)}
			return
		}
	}
	if option.Id != "" {
		sql, err = api.sql.GetByTableAndID(option)
	} else {
		sql, err = api.sql.GetByTable(option)
	}
	if err != nil {
		errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
		return
	}
	return api.query(sql)
}

func (api *MysqlAPI) SelectTotalCount(option QueryOption) (totalCount int,errorMessage *ErrorMessage) {
	var sql string
	var err error
	for _, f := range option.Fields {
		if !api.databaseMetadata.TableHaveField(option.Table, f) {
			errorMessage = &ErrorMessage{ERR_PARAMETER,fmt.Sprintf("Table '%s' not have '%s' field !", option.Table, f)}
			return
		}
	}
	if option.Id == "" {
		sql, err = api.sql.GetByTableTotalCount(option)
		if err != nil {
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
			return
		}
	} else {
		errorMessage = &ErrorMessage{ERR_PARAMETER,"Id must empty"}
		return
	}

	var data []map[string]interface{}
	data, errorMessage = api.query(sql)
	if errorMessage != nil {
		return
	}
	if len(data) != 1 {
		errorMessage = &ErrorMessage{ERR_SQL_RESULTS,fmt.Sprintf("Expected one result to be returned by selectOne(), but found: %d", len(data))}
		return
	}
	str, _ := data[0]["TotalCount"].(string)
	totalCount, _ = strconv.Atoi(str)
	return
}
