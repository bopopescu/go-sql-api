package mysql

import (
	"fmt"
	"strings"
)

var ERR_SQL_EXECUTION = "err_sql_execution"
var ERR_SQL_RESULTS = "err_sql_results"
var ERR_PARAMETER = "err_parameter"
var ERR_JSONCONVERT="err_json_convert"

// ErrorMessage
type ErrorMessage struct {
	ErrorTitle string      `json:"error"`
	ErrorDescription    string `json:"error_description"`
}
// Error makes it compatible with `error` interface.
func (em *ErrorMessage) Error() string {
	return fmt.Sprintf("error=%s, error_description=%s", em.ErrorTitle, em.ErrorDescription)
}

type Paginator struct {
	PageIndex  int `json:"pageIndex"`
	PageSize int `json:"pageSize"`
	TotalPages int `json:"totalPages"`
	TotalCount int `json:"totalCount"`
	Data    interface{} `json:"data"`
}
// DataBaseMetadata metadata of a database
type DataBaseMetadata struct {
	DatabaseName string           `json:"database_name,omitempty"` // database name
	Tables       []*TableMetadata `json:"tables,omitempty"`        // collection of tables
}

// TableMetadata metadata of a Table
type TableMetadata struct {
	TableName    string            `json:"table_name,omitempty"` //Table name
	TableType    string            `json:"table_type,omitempty"`
	TableRows    int64             `json:"table_rows,omitempty"`
	CurrentIncre int64             `json:"current_increment,omitempty"`
	Comment      string            `json:"comment,omitempty"`
	Columns      []*ColumnMetadata `json:"columns,omitempty"` //collections of column
}

// ColumnMetadata metadata of a column
type ColumnMetadata struct {
	ColumnName string `json:"column_name,omitempty"` // column name or code ?
	ColumnType string `json:"column_type,omitempty"` // column type
	NullAble   string `json:"nullable,omitempty"`    // column null able
	// If Key is PRI, the column is a PRIMARY KEY or is one of the
	// columns in a multiple-column PRIMARY KEY.

	// If Key is UNI, the column is the first column of a unique-valued
	// index that cannot contain NULL values.

	// If Key is MUL, multiple occurrences of a given value are
	// permitted within the column. The column is the first column
	// of a nonunique index or a unique-valued index that can contain
	// NULL values.
	Key              string `json:"key,omitempty"`           // column key type
	DefaultValue     string `json:"default_value,omitempty"` // default value if have
	Extra            string `json:"extra,omitempty"`         // extra info, for example, auto_increment
	OridinalSequence int64  `json:"oridinal_sequence,omitempty"`
	DataType         string `json:"data_type,omitempty"`
	Comment          string `json:"comment,omitempty"`
}

// QueryConfig for Select method
type QueryOption struct {
	Table  string                    // table name
	Id     string                    // select with primary key value
	Index int                       // start page
	Limit  int                       // record limit
	Offset int                       // start offset
	Fields []string                  // select fields
	FieldsType string                  // select fields type
	GroupFunc    string				// 聚合函数查询
	GroupFields    []string	// 分组查询
	ExtendedArr    []map[string]interface{}  //扩展数组
	ExtendedMap    map[string]interface{} // 扩展map
	ExtendedMapSecond    map[string]interface{} // 扩展map
	PriKey     string                    // primary key
	Ids    []string	// ids
	Links  []string                  // auto join table
	Wheres map[string]WhereOperation // field -> { operation, value }
	OrWheres map[string]WhereOperation // field -> { operation, value }
	Orders map[string]string // field -> { operation, value }
	Search string                    // fuzzy query word
}
// 聚合函数 MAX() MIN() SUM() AVG() COUNT()
//type GroupFunc struct {
//	FuncName string
//	Value     interface{}
//}
type WhereOperation struct {
	Operation string
	Value     interface{}
}

func (c *ColumnMetadata) GetDefaultValue() (v interface{}) {
	if c.DefaultValue != "" {
		v = c.DefaultValue
	}
	return
}

// GetTableMeta
func (d *DataBaseMetadata) GetTableMeta(tableName string) *TableMetadata {
	for _, table := range d.Tables {
		if table.TableName == tableName {
			return table
		}
	}
	return nil
}

// GetSimpleMetadata
func (d *DataBaseMetadata) GetSimpleMetadata() (rt map[string]interface{}) {
	rt = make(map[string]interface{})
	for _, table := range d.Tables {
		cls := make([]interface{}, len(table.Columns))
		for idx, i_column := range table.Columns {
			cls[idx] = fmt.Sprintf("%s %s %s NullAble(%s) '%s'", i_column.ColumnName, i_column.ColumnType, i_column.DefaultValue, i_column.NullAble, i_column.Comment)
		}
		rt[fmt.Sprintf("[%s] (%d rows) %s", table.TableType, table.TableRows, table.TableName)] = cls
	}
	return
}

// GetPrimaryColumn
//func (t *TableMetadata) GetPrimaryColumn() *ColumnMetadata {
//	primaryColumns:=t.GetPrimaryColumns()
//	if(len(primaryColumns)>0){
//		return primaryColumns[0]
//	}
//	return nil
//}
// GetPrimaryColumns
func (t *TableMetadata) GetPrimaryColumns() (primaryColumns []*ColumnMetadata) {
	primaryColumns = make([]*ColumnMetadata,0, len(t.Columns))
	for _, col := range t.Columns {
		if col.Key == "PRI" {
			primaryColumns=append(primaryColumns,col);
		}
	}
	return
}

// HaveField
func (t *TableMetadata) HaveField(sFieldName string) bool {
	for _, col := range t.Columns {
		if sFieldName == col.ColumnName {
			return true
		}
	}
	return false
}

// HaveTable
func (d *DataBaseMetadata) HaveTable(sTableName string) bool {
	if t := d.GetTableMeta(sTableName); t != nil {
		return true
	}
	return false
}

// TableHaveField
func (d *DataBaseMetadata) TableHaveField(sTableName string, sFieldName string) bool {
	t := d.GetTableMeta(sTableName)
	if(strings.Contains(sFieldName,".")){
		t=d.GetTableMeta(strings.Split(sFieldName,".")[0])
		sFieldName=strings.Split(sFieldName,".")[1]
	}
	if strings.Contains(sFieldName,"SUM")||strings.Contains(sFieldName,"MAX")||strings.Contains(sFieldName,"MIN")||strings.Contains(sFieldName,"AVG"){
		return true
	}
	if t == nil {
		return false
	}
	return t.HaveField(sFieldName)
}
