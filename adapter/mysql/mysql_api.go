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
	"encoding/json"

	//"github.com/mkideal/pkg/option"
	"container/list"
	"github.com/satori/go.uuid"
	"bytes"
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
// batch Create related table by Table name and obj map
func (api *MysqlAPI) RelatedCreate(operates []map[string]interface{},obj map[string]interface{}) (rowAffect int64,errorMessage *ErrorMessage) {

 	var rowAaffect int64
	var masterRowAffect int64
	var slaveRowAffect int64
	var	rs sql.Result
	var masterId string

	slaveIds := list.New()
	masterTableName:=obj["masterTableName"].(string)
	slaveTableName:=obj["slaveTableName"].(string)
	masterTableInfo:=obj["masterTableInfo"].(string)
	slaveTableInfo:=obj["slaveTableInfo"].(string)
	fmt.Printf("masterTableInfo=",masterTableInfo)
	masterInfoMap:=make(map[string]interface{})
	var slaveInfoMap []map[string]interface{}
	//slaveInfoMap:=make([]map[string]interface{})

	masterInfoMap,errorMessage=Json2map(masterTableInfo)
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
	primaryColumns=api.GetDatabaseMetadata().GetTableMeta(masterTableName).GetPrimaryColumns()

	primaryColumns1=api.GetDatabaseMetadata().GetTableMeta(slaveTableName).GetPrimaryColumns()
	// 如果是一对一 且有相互依赖
	if len(slaveInfoMap)==1 {
		for _, slave := range slaveInfoMap {
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
	if api.GetDatabaseMetadata().GetTableMeta(masterTableName).HaveField(slavePriKey){
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
			if masterTableName=="order_form"{
				masterPriKey=col.ColumnName
				now:=time.Now()

				baseUnix:=strconv.FormatInt(now.Unix(),10)

				t := now.Unix()
				fmt.Println(t)
				//时间戳到具体显示的转化
				fmt.Println(time.Unix(t, 0).String())
				timeStr:=time.Unix(t, 0).String()
				timeStr=string(timeStr[:10])
				timeStr=strings.Replace(timeStr,"-","",-1)
				orderid:=timeStr+baseUnix
				fmt.Printf("tt",orderid)
				masterId=orderid //strconv.Itoa(time.Now().Unix())
			}else{
				masterPriKey=col.ColumnName
				if masterInfoMap[masterPriKey]==nil{
					uuid := uuid.NewV4()
					masterId=uuid.String()
					masterInfoMap[masterPriKey]=masterId
				}else{
					masterId=masterInfoMap[masterPriKey].(string)
				}

			}

			break;//取第一个主键
		}
	}
	masterInfoMap[masterPriKey]=masterId

	if errorMessage!=nil{
		fmt.Printf("err=",errorMessage)
	}
	fmt.Printf("slaveTableName",slaveTableName)
	fmt.Printf("slaveInfoMap",slaveInfoMap)

	sql, err := api.sql.InsertByTable(masterTableName, masterInfoMap)

	if err != nil {

		// 回滚已经插入的数据
	//	api.Delete(masterTableName,masterId,nil)
	//	for e := slaveIds.Front(); e != nil; e = e.Next() {
	//		api.Delete(slaveTableName,e.Value,nil)
	//	}
		errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
		return 0,errorMessage
	}

	rs,errorMessage=api.exec(sql)
	fmt.Printf("batch-master-err=",errorMessage)
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
		fmt.Printf("batch-master-getrows-err",err)
		// 回滚已经插入的数据
		api.Delete(masterTableName,masterId,nil)
		for e := slaveIds.Front(); e != nil; e = e.Next() {
			api.Delete(slaveTableName,e.Value,nil)
		}
		errorMessage = &ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+err.Error()}
		return 0,errorMessage
	}
	var operate_type string
	var operate_table string
	var calculate_field string
	var calculate_func string
	var conditionFileds string
	var conditionFileds1 string
	var funcParamFieldStr string
	var operateCondJsonMap map[string]interface{}
	var operateCondContentJsonMap map[string]interface{}
	asyncObjectMap:=make(map[string]interface{})//构建同步数据对象
	var conditionFiledArr [10]string
	var conditionFiledArr1 [10]string
	//conditionFiledArr := list.New()
	//conditionFiledArr1 := list.New()
	var funcParamFields [10]string

	for _, slave := range slaveInfoMap {
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
		if slavePriId==""{
			uuid := uuid.NewV4()
			slavePriId=uuid.String()
			slave[slavePriKey]=slavePriId
		}else {
			slave[slavePriKey]=slavePriId
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
			return 0,errorMessage
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
				return 0,errorMessage
			}else{
				slaveRowAffect,err=rs.RowsAffected()

				for _,operate:=range operates {
					operate_condition := operate["operate_condition"].(string)
					operate_content := operate["operate_content"].(string)

					if (operate_condition != "") {
						json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)
						conditionFileds = operateCondJsonMap["conditionFields"].(string)
						conditionFileds1 = operateCondJsonMap["conditionFieldss"].(string)
						funcParamFieldStr = operateCondJsonMap["funcParamFields"].(string)
						json.Unmarshal([]byte(conditionFileds), &conditionFiledArr)
						json.Unmarshal([]byte(conditionFileds1), &conditionFiledArr1)
						json.Unmarshal([]byte(funcParamFieldStr), &funcParamFields)
					}
					if (operate_content != "") {
						json.Unmarshal([]byte(operate_content), &operateCondContentJsonMap)
					}
					if operateCondContentJsonMap!=nil{
						operate_type = operateCondContentJsonMap["operate_type"].(string)
						operate_table = operateCondContentJsonMap["operate_table"].(string)
						calculate_field=operateCondContentJsonMap["calculate_field"].(string)
						calculate_func=operateCondContentJsonMap["calculate_func"].(string)
					}

					//如果是 operate_type ASYNC_BATCH_SAVE 同步批量保存并计算值
					if "ASYNC_BATCH_SAVE"==operate_type{
						asyncObjectMap=buildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=buildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)
						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if calculate_func!=""{
							//如果执行方法不为空 执行配置中方法
							paramsMap=buildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=buildMapFromBody(funcParamFields,slave,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=concatObjectProperties(funcParamFields,paramsMap)
							calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+"),2) as result;"

							rs,error:= api.ExecFunc(calculate_func_sql_str)
							//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
							fmt.Printf("error",error)
							fmt.Printf("rs",rs)
							var result string
							for _,item:=range rs{
								fmt.Printf("")
								result=item["result"].(string)
							}
							asyncObjectMap[calculate_field]=result
						}
						api.Create(operate_table,asyncObjectMap)

					}

					// ASYNC_BATCH_SAVE_CURRENT_PEROID 计算指定配置的值
					if "ASYNC_BATCH_SAVE_CURRENT_PEROID"==operate_type{
						asyncObjectMap=buildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=buildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)
						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if calculate_func!=""{
							// SELECT DATE_FORMAT(LAST_DAY(CURDATE()),'%Y-%m-%d') AS last_date;
							laste_date_sql:="SELECT DATE_FORMAT(LAST_DAY(CURDATE()),'%Y-%m-%d') AS last_date;"
							result1:=api.ExecFuncForOne(laste_date_sql,"last_date")
							masterInfoMap["account_period_year"]=result1

							asyncObjectMap["voucher_type"]=nil
							asyncObjectMap["subject_key"]=nil
							asyncObjectMap["line_number"]=100
							asyncObjectMap["order_num"]=nil
							asyncObjectMap["summary"]="本期合计"
							asyncObjectMap["account_period_year"]=result1
							//如果执行方法不为空 执行配置中方法
							paramsMap=buildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=buildMapFromBody(funcParamFields,slave,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=concatObjectProperties(funcParamFields,paramsMap)



							// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
							judgeExistsSql:="select judgeCurrentPeroidExists("+paramStr+") as id;"
							if strings.Contains(calculate_field,","){
								fields:=strings.Split(calculate_field,",")
								for index,item:=range fields{
									calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
									result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
									//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")

									asyncObjectMap[item]=result

								}
							}



							id:=api.ExecFuncForOne(judgeExistsSql,"id")
							if id==""{
								asyncObjectMap["id"]=asyncObjectMap["id"].(string)+"-peroid"
								api.Create(operate_table,asyncObjectMap)
							}else{//id不为空 则更新
								asyncObjectMap["id"]=id
							   r,errorMessage:= api.Update(operate_table,id,asyncObjectMap)
							   if errorMessage!=nil{
							   	fmt.Printf("errorMessage=",errorMessage)
							   }
							   fmt.Printf("rs=",r)

							}



						}


					}

					// ASYNC_BATCH_SAVE_CURRENT_YEAR
					if "ASYNC_BATCH_SAVE_CURRENT_YEAR"==operate_type{
						asyncObjectMap=buildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=buildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)
						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if calculate_func!=""{
							// SELECT DATE_FORMAT(LAST_DAY(CURDATE()),'%Y-%m-%d') AS last_date;
							laste_date_sql:="SELECT DATE_FORMAT(LAST_DAY(CURDATE()),'%Y-%m-%d') AS last_date;"
							result1:=api.ExecFuncForOne(laste_date_sql,"last_date")
							masterInfoMap["account_period_year"]=result1

							asyncObjectMap["voucher_type"]=nil
							asyncObjectMap["subject_key"]=nil
							asyncObjectMap["order_num"]=nil
							asyncObjectMap["line_number"]=101
							asyncObjectMap["summary"]="本年累计"
							asyncObjectMap["account_period_year"]=result1
							//如果执行方法不为空 执行配置中方法
							paramsMap=buildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=buildMapFromBody(funcParamFields,slave,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=concatObjectProperties(funcParamFields,paramsMap)



							// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
							judgeExistsSql:="select judgeCurrentYearExists("+paramStr+") as id;"
							if strings.Contains(calculate_field,","){
								fields:=strings.Split(calculate_field,",")
								for index,item:=range fields{
									calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
									result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
									//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")

									asyncObjectMap[item]=result

								}
							}



							id:=api.ExecFuncForOne(judgeExistsSql,"id")
							if id==""{
								asyncObjectMap["id"]=asyncObjectMap["id"].(string)+"-year"
								api.Create(operate_table,asyncObjectMap)
							}else{//id不为空 则更新
								asyncObjectMap["id"]=id
								r,errorMessage:= api.Update(operate_table,id,asyncObjectMap)
								if errorMessage!=nil{
									fmt.Printf("errorMessage=",errorMessage)
								}
								fmt.Printf("rs=",r)

							}



						}


					}

				}



			}
			rowAaffect=rowAaffect+slaveRowAffect
		}

	}
	rowAaffect=rowAaffect+masterRowAffect
  return rowAaffect,nil
}
func (api *MysqlAPI)ExecFuncForOne(sql string,key string)(string){
	rs,error:= api.ExecFunc(sql)
	//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
	fmt.Printf("error",error)
	fmt.Printf("rs1",rs)
	var result string
	for _,item:=range rs{
		fmt.Printf("")
		if item[key]!=nil{
			result=item[key].(string)
		}

	}
return result
}
func concatObjectProperties(funcParamFields [10]string,object map[string]interface{})(string){
	var resultStr string
	b := bytes.Buffer{}
	for _,item:=range funcParamFields{
		if item!=""&&object[item]!=nil{
			b.WriteString(object[item].(string)+",")
		}


	}
	resultStr="'"+strings.Replace(b.String(),",","','",-1)+"'"
	resultStr=strings.Replace(resultStr,",''","",-1)
	return resultStr
}

func buildMapFromBody(properties [10]string,fromObjec map[string]interface{},disObjec map[string]interface{})(map[string]interface{}){
	for _,item:=range properties{
		if item!=""&&fromObjec[item]!=nil{
			disObjec[item]=fromObjec[item].(string)
		}
	}
	return disObjec;
}

func (api *MysqlAPI) RelatedUpdate(obj map[string]interface{}) (rowAffect int64,errorMessage *ErrorMessage) {
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
	primaryColumns=api.GetDatabaseMetadata().GetTableMeta(masterTableName).GetPrimaryColumns()
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

	for i, slave := range slaveInfoMap {
		sql, err := api.sql.UpdateByTableAndId(slaveTableName,slave["id"].(string), slave)
		fmt.Printf("i=",i)
		slaveIds.PushBack(slave["id"].(string))

		if err!=nil{
			// 回滚已经插入的数据
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
			return 0,errorMessage
		}else{
			rs,errorMessage=api.exec(sql)
			
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
func (api *MysqlAPI) UpdateBatch(table string, where map[string]WhereOperation, obj map[string]interface{}) (rs sql.Result,errorMessage *ErrorMessage) {

		sql, err := api.sql.UpdateByTableAndFields(table, where, obj)
		if err != nil {
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
			return
		}
		return api.exec(sql)

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
