package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/labstack/gommon/log"
	"github.com/shiyongabc/go-sql-api/adapter"
	"github.com/shiyongabc/go-sql-api/server/lib"
	. "github.com/shiyongabc/go-sql-api/types"
	"gopkg.in/doug-martin/goqu.v4"
	_ "gopkg.in/doug-martin/goqu.v4/adapters/mysql"
	"regexp"
	"strconv"
	"strings"
	"time"

	"bytes"
	//"github.com/mkideal/pkg/option"
	"container/list"
	"github.com/satori/go.uuid"

	"math/rand"
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
func (api *MysqlAPI) GetDatabaseMetadataWithView() *DataBaseMetadata {
	meta:=api.databaseMetadata
	for _, t := range meta.Tables {
		if t.TableType == "VIEW" && t.Comment != "" {

			//
			wMap := map[string]WhereOperation{}
			wMap["view_name"] = WhereOperation{
				Operation: "eq",
				Value:     t.TableName,
			}
			//option.Wheres=wMap
			//option.Table=t.TableName
			//api := mysql.NewMysqlAPI(main.connectionStr, main.useInformationSchema)
			option := QueryOption{Wheres: wMap, Table: "view_config"}
			data, errorMessage := api.Select(option)
			fmt.Printf("data", data)
			fmt.Printf("errorMessage", errorMessage)
			for _,view:=range data {
				t.Comment = view["view_des"].(string)
			}





		}
	}
	return meta
}

// GetDatabaseMetadata return database meta
func (api *MysqlAPI) GetDatabaseMetadata() *DataBaseMetadata {
	meta:=api.databaseMetadata
	return meta
}

func (api *MysqlAPI) GetDatabaseTableMetadata(tableName string) *TableMetadata {
	meta:=api.databaseMetadata

		if meta.GetTableMeta(tableName)!=nil {
			//给视图的列显示comment(mysql view不支持添加列的comment 除非原始表里面有comment 如果原始表没有 则没有comment内容)

			wMap:= map[string]WhereOperation{}
			wMap["view_name"] = WhereOperation{
				Operation: "eq",
				Value:     tableName,
			}

			for _,item:=range meta.GetTableMeta(tableName).Columns{
				columnName:=item.ColumnName
				wMap["view_column"] = WhereOperation{
					Operation: "eq",
					Value:     columnName,
				}
				option := QueryOption{Wheres: wMap, Table: "view_detail_config"}
				if item.Comment==""{
					dataDetail, errorMessage := api.Select(option)
					fmt.Printf("dataDetail", dataDetail)
					fmt.Printf("errorMessage", errorMessage)
					var viewColumnComment string
					for _,view:=range dataDetail {
						viewColumnComment = view["column_comment"].(string)
						item.Comment=viewColumnComment
						break
					}

				}

			}




			}

	return meta.GetTableMeta(tableName)
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
func (api *MysqlAPI)ExecFunc(sql string) (rs []map[string]interface{},errorMessage *ErrorMessage){
	//api.exec(sql,params)
	return api.query(sql)
}
func (api *MysqlAPI)ExecSql(sql string) (rs []map[string]interface{},errorMessage *ErrorMessage){
	//api.exec(sql,params)
	return api.query(sql)
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
		pool.SetMaxIdleConns(6)
		pool.SetMaxOpenConns(15)
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
		reg:= regexp.MustCompile(`^*[_]\d{4}$`)
		resuts:=reg.FindAllString(tableName, -1)
		if len(resuts)>0 || tableName=="operate_config"{
			continue
		}
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
		reg:= regexp.MustCompile(`^*[_]\d{4}$`)
		resuts:=reg.FindAllString(tableName.String, -1)
		if len(resuts)>0 || tableName.String=="operate_config"{
			continue
		}
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
	//sql="SELECT `user_id`, `SUM(account_log`.`account_funds) as totalFunds` FROM `account_log`"
//	stmt,error:=api.connection.Prepare(sql)
  //  rows0,error:=stmt.Query(sql)
//	fmt.Printf("rows1",rows0,error)
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
// Create by Table name and obj map
func (api *MysqlAPI) CreateSql(table string, obj map[string]interface{}) (sql string,errorMessage *ErrorMessage) {
	sql, err := api.sql.InsertByTable(table, obj)
	if err != nil {
		errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
	}
	return sql,errorMessage
}
// Create by Table name and obj map
func (api *MysqlAPI) ReplaceCreate(table string, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage) {
	sql, err := api.sql.InsertByTable(table, obj)
	sql=strings.Replace(sql,"INSERT","REPLACE",-1)
	if err != nil {
		errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
	}
	return api.exec(sql)
}
func  Json2map(jsonStr string) (s map[string]interface{}, errorMessage *ErrorMessage) {
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		errorMessage = &ErrorMessage{ERR_JSONCONVERT,"json convert error:"+err.Error()}
		return nil, errorMessage
	}
	return result, nil
}
func  JsonArr2map(jsonArrStr string) (s []map[string]interface{},errorMessage *ErrorMessage) {
	var result []map[string]interface{}
	if err := json.Unmarshal([]byte(jsonArrStr), &result); err != nil {
		errorMessage = &ErrorMessage{ERR_JSONCONVERT,"json convert error:"+err.Error()}
		return nil, errorMessage
	}
	return result, nil
}
func GenerateRandnum() int {
	rand.Seed(time.Now().Unix())
	randNum := rand.Intn(100)
	return randNum
}
// batch Create related table by Table name and obj map
func (api *MysqlAPI) RelatedCreate(operates []map[string]interface{},obj map[string]interface{},submitPerson string) (rowAffect int64,masterKey string,masterId string,errorMessage *ErrorMessage) {

 	var rowAaffect int64
	var masterRowAffect int64
	var slaveRowAffect int64
	var	rs sql.Result
	//var masterId string

	slaveIds := list.New()
	masterTableName:=obj["masterTableName"].(string)
	slaveTableName:=obj["slaveTableName"].(string)
	masterTableInfo:=obj["masterTableInfo"]

	slaveTableInfo:=obj["slaveTableInfo"].(string)
	fmt.Printf("masterTableInfo=",masterTableInfo)
	masterInfoMap:=make(map[string]interface{})
	var slaveInfoMap []map[string]interface{}
	slaveMetaData:=api.GetDatabaseMetadata().GetTableMeta(slaveTableName)
	//slaveInfoMap:=make([]map[string]interface{})
	if masterTableInfo!=nil{
		if masterTableInfo.(string)!=""{
			masterInfoMap,errorMessage=Json2map(masterTableInfo.(string))
		}

	}


	if errorMessage!=nil{
		fmt.Printf("err=",errorMessage)
	}
	//
	slaveInfoMap,errorMessage=JsonArr2map(slaveTableInfo)

	var primaryColumns []*ColumnMetadata
    var masterPriKey string
	var slavePriId string
	var slavePriKey string

	var primaryColumns1 []*ColumnMetadata
	masterMeta:=api.GetDatabaseMetadata().GetTableMeta(masterTableName)
	slaveMeta:=api.GetDatabaseMetadata().GetTableMeta(slaveTableName)
	primaryColumns=masterMeta.GetPrimaryColumns()
	primaryColumns1=slaveMeta.GetPrimaryColumns()
	if masterMeta.HaveField("create_time"){
		masterInfoMap["create_time"]=time.Now().Format("2006-01-02 15:04:05")
	}

	if masterMeta.HaveField("submit_person"){
		masterInfoMap["submit_person"]=submitPerson
	}
	// 如果是一对一 且有相互依赖
	if len(slaveInfoMap)==1 {
		for _, slave := range slaveInfoMap {
			if slaveMeta.HaveField("create_time"){
				slave["create_time"]=time.Now().Format("2006-01-02 15:04:05")
			}
			for _, col := range primaryColumns1 {
				if col.Key == "PRI" {
					slavePriKey = col.ColumnName

					if slave[slavePriKey] != nil {
						slavePriId = slave[slavePriKey].(string)
					}
					fmt.Printf("slavePriId-key==", slavePriKey)
					break; //取第一个主键
				}
			}

		}
	}
	if masterMeta.HaveField(slavePriKey){
		uuid := uuid.NewV4()
		//slavePriId=uuid.String()
		if slavePriId == "" {
			slavePriId = uuid.String()
		}
		fmt.Printf("slavePriId====", slavePriId)
		masterInfoMap[slavePriKey] = slavePriId
	}
		

	for _, col := range primaryColumns {
		if col.Key == "PRI" {
				masterPriKey=col.ColumnName
				if masterInfoMap[masterPriKey]==nil || masterInfoMap[masterPriKey]==""{
					uuid := uuid.NewV4()
					masterId=uuid.String()
					masterInfoMap[masterPriKey]=masterId
				}else{
					masterId=masterInfoMap[masterPriKey].(string)
				}

			break;//取第一个主键
		}
	}
	masterKey=masterPriKey
	masterInfoMap[masterPriKey]=masterId

	if errorMessage!=nil{
		fmt.Printf("err=",errorMessage)
	}
	fmt.Printf("slaveTableName",slaveTableName)
	fmt.Printf("slaveInfoMap",slaveInfoMap)
	var sql string
	var err error
    if obj["isCreated"]==nil{
		sql, err = api.sql.InsertByTable(masterTableName, masterInfoMap)
		rs,errorMessage=api.exec(sql)
		masterRowAffect,err=rs.RowsAffected()
		if err != nil {

			// 回滚已经插入的数据
			//	api.Delete(masterTableName,masterId,nil)
			//	for e := slaveIds.Front(); e != nil; e = e.Next() {
			//		api.Delete(slaveTableName,e.Value,nil)
			//	}
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
			return 0,masterPriKey,masterId,errorMessage
		}
		fmt.Printf("err=",err)
	}






	fmt.Printf("batch-master-err=",errorMessage)
	if errorMessage != nil  {
		// 回滚已经插入的数据
		api.Delete(masterTableName,masterId,nil)
		for e := slaveIds.Front(); e != nil; e = e.Next() {
			api.Delete(slaveTableName,e.Value,nil)
		}
		errorMessage = &ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+errorMessage.Error()}
       return 0,masterPriKey,masterId,errorMessage
	}


	if err != nil {
		fmt.Printf("batch-master-getrows-err",err)
		// 回滚已经插入的数据
		api.Delete(masterTableName,masterId,nil)
		for e := slaveIds.Front(); e != nil; e = e.Next() {
			api.Delete(slaveTableName,e.Value,nil)
		}
		errorMessage = &ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+err.Error()}
		return 0,masterKey,masterId,errorMessage
	}

	for _, slave := range slaveInfoMap {
		if slave["extra_info"]!=nil{
			slave["extra_info"]=slave["extra_info"]
			extraBytes,err:=json.Marshal(slave["extra_info"])
			fmt.Print(err)
			extraStr:=string(extraBytes[:])
			slave["extra_info"]=extraStr
		}
		for _, col := range primaryColumns1 {
			if col.Key == "PRI" {
				slavePriKey = col.ColumnName

				if slave[slavePriKey]!=nil{
					slavePriId=slave[slavePriKey].(string)
				}
				fmt.Printf("slave", slave)
				break; //取第一个主键
			}
		}
		//设置主键id
        slave[masterPriKey]=masterId
		//if slavePriId==""{
			uuid := uuid.NewV4()
			slavePriId=uuid.String()
			slave[slavePriKey]=slavePriId
	//	}else {
		//	slave[slavePriKey]=slavePriId
		//}
		if slaveMetaData.HaveField("create_time"){
			slave["create_time"]=time.Now().Format("2006-01-02 15:04:05")
		}


		sql, err := api.sql.InsertByTable(slaveTableName, slave)
		fmt.Printf("get-sql-err=",err)
		fmt.Printf("slavePriId",slavePriId)
		slaveIds.PushBack(slavePriId)

		if err!=nil{
			// 回滚已经插入的数据
			api.Delete(masterTableName,masterId,nil)
			for e := slaveIds.Front(); e != nil; e = e.Next() {
				api.Delete(slaveTableName,e.Value,nil)
			}
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
			return 0,masterKey,masterId,errorMessage
		}else{
			rs,errorMessage=api.exec(sql)

			if errorMessage != nil {
				fmt.Printf("batch-slave-err",errorMessage)
				// 回滚已经插入的数据
				api.Delete(masterTableName,masterId,nil)
				for e := slaveIds.Front(); e != nil; e = e.Next() {
					api.Delete(slaveTableName,e.Value,nil)
				}

				errorMessage = &ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+err.Error()}
				return 0,masterKey,masterId,errorMessage
			}else{
				slaveRowAffect,err=rs.RowsAffected()

			}
			rowAaffect=rowAaffect+slaveRowAffect
		}

	}
	rowAaffect=rowAaffect+masterRowAffect
  return rowAaffect,masterKey,masterId,nil
}
func SelectOperaInfo(api adapter.IDatabaseAPI,tableName string,apiMethod string,isAsyncTask string) (rs []map[string]interface{},errorMessage *ErrorMessage) {

	whereOption := map[string]WhereOperation{}
	whereOption["cond_table"] = WhereOperation{
		Operation: "eq",
		Value:     tableName,
	}
	whereOption["api_method"] = WhereOperation{
		Operation: "eq",
		Value:     apiMethod,
	}
	// is_async_task 0 不是异步任务
	whereOption["is_async_task"] = WhereOperation{
		Operation: "eq",
		Value:     isAsyncTask,
	}
	querOption := QueryOption{Wheres: whereOption, Table: "operate_config"}
	orders:=make(map[string]string)
	orders["order_num"]="asc"
	querOption.Orders=orders
	rs, errorMessage= api.Select(querOption)
	if errorMessage!=nil{
		fmt.Printf("errorMessage", errorMessage)
	}else{
		fmt.Printf("rs", rs)
	}

	return rs,errorMessage
}

func ObtainDefineLocal(api adapter.IDatabaseAPI,defineId string,value string) (rowStr string,errorMessage *ErrorMessage) {

	whereOptionLocal := map[string]WhereOperation{}
	whereOptionLocal["report_type"] = WhereOperation{
		Operation: "eq",
		Value:     defineId,
	}
	whereOptionLocal["value"] = WhereOperation{
		Operation: "eq",
		Value:     value,
	}
	querOptionLocal := QueryOption{Wheres: whereOptionLocal, Table: "report_diy_cells"}

	rsLocal, errorMessage:= api.Select(querOptionLocal)
	fmt.Printf("errorMessage=",errorMessage)
	for _,item:=range rsLocal{
		switch item["row"].(type) {
		case string:
			rowStr=item["row"].(string)
		case int:
			rowStr=strconv.Itoa(item["row"].(int))
		}

		break;
	}
	return rowStr,errorMessage
}

func SelectOperaInfoByAsyncKey(api adapter.IDatabaseAPI,asyncKey string) (rs []map[string]interface{},errorMessage *ErrorMessage) {

	whereOption := map[string]WhereOperation{}
	whereOption["async_key"] = WhereOperation{
		Operation: "eq",
		Value:     asyncKey,
	}

	querOption := QueryOption{Wheres: whereOption, Table: "operate_config"}
	rs, errorMessage= api.Select(querOption)
	if errorMessage!=nil{
		fmt.Printf("errorMessage", errorMessage)
	}else{
		fmt.Printf("rs", rs)
	}

	return rs,errorMessage
}

func SelectOperaInfoByOperateKey(api adapter.IDatabaseAPI,operate_key string) (rs []map[string]interface{},errorMessage *ErrorMessage) {

	whereOption := map[string]WhereOperation{}
	whereOption["operate_key"] = WhereOperation{
		Operation: "eq",
		Value:     operate_key,
	}

	querOption := QueryOption{Wheres: whereOption, Table: "operate_config"}
	orders:=make(map[string]string)
	orders["order_num"]="asc"
	querOption.Orders=orders
	rs, errorMessage= api.Select(querOption)
	if errorMessage!=nil{
		fmt.Printf("errorMessage", errorMessage)
	}else{
		fmt.Printf("rs", rs)
	}

	return rs,errorMessage
}

func (api *MysqlAPI)ExecFuncForOne(sql string,key string)(result string,errorMessage *ErrorMessage){
	rs,errorMessage:= api.ExecFunc(sql)
	//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
	fmt.Printf("error",errorMessage)
	fmt.Printf("rs1",rs)
	for _,item:=range rs{
		fmt.Printf("")
		if item[key]!=nil{
			result=item[key].(string)
		}

	}
return result,errorMessage
}
func ConverStrFromMap(key string,mm map[string]interface{})(string){
	b := bytes.Buffer{}

		if mm!=nil&&mm[key]!=nil{
			switch mm[key].(type) {      //多选语句switch
			case string:
				//是字符时做的事情
				b.WriteString(mm[key].(string))
			case float64:
				//是整数时做的事情
				b.WriteString(strconv.FormatFloat(mm[key].(float64), 'f', -1, 64))

			}


	}

	return b.String()
}
func BuildObjectProperties(funcParamFields []string,object map[string]interface{},actionParamFields []string)([]byte){
	 resultMap:=make(map[string]string)
	for index,item:=range funcParamFields{
		if item!="" {
			// 如果有指定表 截断表名
			if strings.Contains(item,"."){
				item=strings.Split(item,".")[1]
			}
			switch object[item].(type) { //多选语句switch
			case string:
				//是字符时做的事情
				if object[item]==nil || object[item].(string)==""{
					resultMap[actionParamFields[index]]=""
				}else{
					resultMap[actionParamFields[index]]=object[item].(string)
				}

			case float64:
				//是整数时做的事情
				// b.WriteString(strconv.FormatFloat(object[item].(float64), 'f', -1, 64) + ",")
				resultMap[actionParamFields[index]]=strconv.FormatFloat(object[item].(float64), 'f', -1, 64) + ","
			}
		}

	}
	fmt.Print("resultMap",resultMap)
	orderBytes,err:=json.Marshal(resultMap)
	fmt.Print("err",err)
	return orderBytes
}
func ConcatObjectProperties(funcParamFields []string,object map[string]interface{})(string){
	var resultStr string
	b := bytes.Buffer{}
	for _,item:=range funcParamFields{
		if item!="" {
			// 如果有指定表 截断表名
			if strings.Contains(item,"."){
				item=strings.Split(item,".")[1]
			}
			if object[item]==nil{
				b.WriteString("''" + ",")
			}
			switch object[item].(type) { //多选语句switch
			case string:
				//是字符时做的事情
				if object[item]==nil || object[item].(string)==""{
					b.WriteString("''" + ",")
				}else{
					b.WriteString(object[item].(string) + ",")
				}

			case float64:
				//是整数时做的事情
				b.WriteString(strconv.FormatFloat(object[item].(float64), 'f', -1, 64) + ",")

			}
		}



	}
	resultStr="'"+strings.Replace(b.String(),",","','",-1)+"'"
	resultStr=strings.Replace(resultStr,",''","",-1)
	resultStr=strings.Replace(resultStr,"'''","',''",-1)
	resultStr=strings.Replace(resultStr,"''''","'',''",-1)
	// ''','''    '','',''
	resultStr=strings.Replace(resultStr,"''','''","'','',''",-1)
	return resultStr
}
func BuildMapFromObj(fromObjec map[string]interface{},disObjec map[string]interface{})(map[string]interface{}){
	for k,v:=range fromObjec{
		disObjec[k]=v
	}
	return disObjec;
}

func BuildMapFromBody(properties []string,fromObjec map[string]interface{},disObjec map[string]interface{})(map[string]interface{}){
	for _,item:=range properties{
		if item!=""&&fromObjec[item]!=nil{

			switch fromObjec[item].(type) {      //多选语句switch
			case string:
				//是字符时做的事情
				disObjec[item]=fromObjec[item].(string)
			case float64:
				//是整数时做的事情
				disObjec[item]=fromObjec[item].(float64)
			case int:
				//是整数时做的事情
				disObjec[item]=fromObjec[item].(int)
			}

		}
	}
	return disObjec;
}

func (api *MysqlAPI) RelatedUpdate(operates []map[string]interface{},obj map[string]interface{},updatePerson string) (rowAffect int64,errorMessage *ErrorMessage) {
	var rowAaffect int64
	var masterRowAffect int64
	var slaveRowAffect int64
	var	rs sql.Result
	var masterId string
	var masterKeyColName string
	slaveIds := list.New()
	masterTableName:=obj["masterTableName"].(string)
	slaveTableName:=obj["slaveTableName"].(string)
	masterTableInfo:=obj["masterTableInfo"].(string)
	slaveTableInfo:=obj["slaveTableInfo"].(string)
	fmt.Printf("masterTableInfo=",masterTableInfo)
	masterInfoMap:=make(map[string]interface{})
	var slaveInfoMap []map[string]interface{}

	//slaveInfoMap:=make([]map[string]interface{})
	var primaryColumns []*ColumnMetadata
	masterMeta:=api.GetDatabaseMetadata().GetTableMeta(masterTableName)
	primaryColumns=masterMeta.GetPrimaryColumns()
	for _, col := range primaryColumns {
		if col.Key == "PRI" {
			masterKeyColName=col.ColumnName
			break;//取第一个主键
		}
	}
	masterInfoMap,errorMessage=Json2map(masterTableInfo)
	if errorMessage!=nil{
		fmt.Printf("err=",errorMessage)
	}
	masterId=masterInfoMap[masterKeyColName].(string)
	//
	slaveInfoMap,errorMessage=JsonArr2map(slaveTableInfo)
	if errorMessage!=nil{
		fmt.Printf("err=",errorMessage)
	}
	fmt.Printf("slaveTableName",slaveTableName)
	fmt.Printf("slaveInfoMap",slaveInfoMap)
	if masterMeta.HaveField("submit_person"){
		masterInfoMap["submit_person"]=updatePerson
	}
	sql, err := api.sql.UpdateByTableAndId(masterTableName,masterId, masterInfoMap)

	if errorMessage != nil {
		errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
		return 0,errorMessage
	}

	rs,errorMessage=api.exec(sql)
	fmt.Printf("err=",errorMessage)
	if errorMessage != nil  {
		// 回滚已经插入的数据
		api.Delete(masterTableName,masterId,nil)
		for e := slaveIds.Front(); e != nil; e = e.Next() {
			api.Delete(slaveTableName,e.Value,nil)
		}
		errorMessage = &ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+errorMessage.Error()}
		return 0,errorMessage
	}

	masterRowAffect,err=rs.RowsAffected()
	if err != nil {

		errorMessage = &ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+err.Error()}
		return 0,errorMessage
	}
	//var masterOrderNum int
	//if masterInfoMap["order_num"]!=nil{
		//masterOrderNum,_=strconv.Atoi(masterInfoMap["order_num"].(string))
	//}

	// 查询 被删除id
	b := bytes.Buffer{}
	for _, slave := range slaveInfoMap {
		if slave["id"]!=nil{
			b.WriteString(slave["id"].(string)+",")
		}

	}
	inParams:="'"+strings.Replace(b.String(),",","','",-1)+"'"
	inParams=strings.Replace(inParams,",''","",-1)
	inParams=strings.Replace(inParams,"\\'","'",-1)
	inParams=strings.Replace(inParams,"''","'",-1)
	inParams=strings.Replace(inParams,"'","",-1)
	inParams=strings.Replace(inParams,",","','",-1)
	//  subject_key IN ('102\',\'501'))
	var queryOption0 QueryOption

	whereOption0:=make(map[string]WhereOperation)
	whereOption0["id"] = WhereOperation{
		Operation: "notIn",
		Value:     inParams,
	}
	whereOption0[masterKeyColName] = WhereOperation{
		Operation: "eq",
		Value:     masterInfoMap[masterKeyColName],
	}
	queryOption0.Wheres=whereOption0
	queryOption0.Table=slaveTableName
	rr,errorMessage:=api.Select(queryOption0)
	for _,item:=range rr{
		var ids []string
		var deleteEdOption QueryOption
		ids=append(ids,item["id"].(string))
		deleteEdOption.Ids=ids
		PreEvent(api,slaveTableName,"DELETE",nil,deleteEdOption,"")
		_,errorMessage:=api.Delete(slaveTableName,item["id"],nil)
		fmt.Printf("errorMessage=",errorMessage)
	}

	for i, slave := range slaveInfoMap {
		if slave["extra_info"]!=nil{
			slave["extra_info"]=slave["extra_info"]
			extraBytes,err:=json.Marshal(slave["extra_info"])
			fmt.Print(err)
			extraStr:=string(extraBytes[:])
			slave["extra_info"]=extraStr
		}


		var updateSql string

		if slave["id"]!=nil{
			if slave["id"].(string)!=""{
				updateSql, err = api.sql.UpdateByTableAndId(slaveTableName,slave["id"].(string), slave)
				rs,errorMessage=api.exec(updateSql)
				fmt.Printf("err=",err)
			}else{
				slave["id"]=uuid.NewV4().String()
				//rs,errorMessage=api.Create(slaveTableName,slave)

				objCreate:=make(map[string]interface{})
				objCreate=obj
				var createSlaveMap []map[string]interface{}
				createSlaveMap=append(createSlaveMap,slave)
				byte,error:=json.Marshal(createSlaveMap)
				fmt.Printf("error=",error)
				objCreate["slaveTableInfo"]=string(byte[:])
				objCreate["isCreated"]="1"
				api.RelatedCreate(operates,objCreate,updatePerson)
				fmt.Printf("rsCreate=",rs)

			}

		}else{
			slave["id"]=uuid.NewV4().String()
			//rs,errorMessage=api.Create(slaveTableName,slave)

			objCreate:=make(map[string]interface{})
			objCreate=obj
			var createSlaveMap []map[string]interface{}
			createSlaveMap=append(createSlaveMap,slave)
			byte,error:=json.Marshal(createSlaveMap)
			fmt.Printf("error=",error)
			objCreate["slaveTableInfo"]=string(byte[:])
			objCreate["isCreated"]="1"
			api.RelatedCreate(operates,objCreate,updatePerson)
			fmt.Printf("rsCreate=",rs)

		}

		fmt.Printf("i=",i)
		slaveIds.PushBack(slave["id"].(string))

		if err!=nil{
			// 回滚已经插入的数据
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
			return 0,errorMessage
		}else{

			if errorMessage != nil {
				errorMessage = &ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+errorMessage.Error()}
				return 0,errorMessage
			}else{
				slaveRowAffect,err=rs.RowsAffected()
				}
			rowAaffect=rowAaffect+slaveRowAffect
		}

	}
	rowAaffect=rowAaffect+masterRowAffect

	return rowAaffect,nil

}

func (api *MysqlAPI) CreateTableStructure(execSql string) (errorMessage *ErrorMessage) {
	r,error:=api.connection.Exec(execSql)
	fmt.Printf("result=",r)
	if error != nil {
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,error.Error()}
			return
	} else {
		return nil
	}
}
func (api *MysqlAPI) UpdateSql(table string, id interface{}, obj map[string]interface{}) (sql string,errorMessage *ErrorMessage) {
	if id != nil {
		sql, err := api.sql.UpdateByTableAndId(table, id, obj)
		if err != nil {
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
			return sql,errorMessage
		}
		return sql,errorMessage
	} else {
		errorMessage = &ErrorMessage{ERR_PARAMETER,"Only primary key updates are supported(must primary id) !"}
		return sql,errorMessage
	}
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
func (api *MysqlAPI) UpdateBatchSql(table string, where map[string]WhereOperation, obj map[string]interface{}) (sql string,errorMessage *ErrorMessage) {

	sql, err := api.sql.UpdateByTableAndFields(table, where, obj)
	if err != nil {
		errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
		return
	}
	return sql,errorMessage

}
func (api *MysqlAPI) UpdateBatch(table string, where map[string]WhereOperation, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage) {

		sql, err := api.sql.UpdateByTableAndFields(table, where, obj)
		if err != nil {
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
			return
		}
		return api.exec(sql)

}
// Delete by Table name and where obj
func (api *MysqlAPI) DeleteSql(table string, id interface{}, obj map[string]interface{}) (sql string,errorMessage *ErrorMessage) {
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
	return sSQL,errorMessage
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
//  如果有枚举字段  自动查询枚举字段的值
    var optionEnum QueryOption
	optionEnum.Table="system_enum_config"
	enumWhere:=make(map[string]WhereOperation)
	enumWhere["table_name"]=WhereOperation{
		Operation:"eq",
		Value:option.Table,
	}
	optionEnum.Wheres=enumWhere
	enumSql, err := api.sql.GetByTable(optionEnum)
	data,_:=api.query(enumSql)
	if len(data)>0{
		for _,column:=range api.databaseMetadata.GetTableMeta(option.Table).Columns{
			option.Fields=append(option.Fields,column.ColumnName)
		}

	}
	for _,item:=range data{
		option.Fields=append(option.Fields,"obtainSystemEnumName("+item["enum_field_name"].(string)+") as "+item["enum_field_name"].(string)+"_value")
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
//func CallFunc(api *MysqlAPI,calculate_field string,calculate_func string,paramStr string,asyncObjectMap map[string]interface{})(map[string]interface{}){
//	if strings.Contains(calculate_field,","){
//		fields:=strings.Split(calculate_field,",")
//		for index,item:=range fields{
//			calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
//			result,errorMessage:= api.ExecFuncForOne(calculate_func_sql_str,"result")
//			//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
//			if result==""{
//				result="0"
//			}
//			asyncObjectMap[item]=result
//		}
//	}
//	return asyncObjectMap
//}