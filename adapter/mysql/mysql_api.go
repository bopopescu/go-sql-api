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
func GenerateRandnum() int {
	rand.Seed(time.Now().Unix())
	randNum := rand.Intn(100)
	return randNum
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
			if masterTableName=="order_form" || masterTableName=="purchase_form"{
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
				timeStr=strings.Replace(timeStr," ","",-1)

				uuid := uuid.NewV4()
				randomStr:=uuid.String()

				orderid:=timeStr+strconv.Itoa(GenerateRandnum())+baseUnix+randomStr[0:5]
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
			return 0,errorMessage
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
       return 0,errorMessage
	}


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
	var slaveKey string
	var summary string
	rebuildSlaveObjectMap:=make(map[string]interface{})//构建同步数据对象
	var rebuildSlaveObjectMapp []map[string]interface{}//构建同步数据对象
	rebuildSlaveCalMap:=make(map[string]interface{})//存放通过func计算出来值
	var conditionFiledArr [10]string
	var conditionFiledArr1 [10]string
	//conditionFiledArr := list.New()
	//conditionFiledArr1 := list.New()
	var funcParamFields [10]string
    var operate_func string
	// 通过 OperateKey查询前置事件
	opK,errorMessage:=SelectOperaInfoByOperateKey(api,masterTableName+"-"+slaveTableName)
    if opK!=nil{
    	for _,item:=range opK{
			operate_condition := item["operate_condition"].(string)
			operate_content := item["operate_content"].(string)

			if (operate_condition != "") {
				json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)
				funcParamFieldStr = operateCondJsonMap["funcParamFields"].(string)
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
				if operateCondContentJsonMap["operate_func"]!=nil{
					operate_func=operateCondContentJsonMap["operate_func"].(string)
				}


			}
		}

		//如果是 operate_type KNOTS_PROFIT_LOSS 结转损益
		//var voucher_type string
		list:=list.New()

		var knots_subject_key string
		var firstSubjectKey string
		if len(slaveInfoMap)>0{
			if slaveInfoMap[0]["knots_subject_key"]!=nil{
				knots_subject_key=slaveInfoMap[0]["knots_subject_key"].(string)
				firstSubjectKey=slaveInfoMap[0]["subject_key"].(string)
			}

		}

			if "KNOTS_PROFIT_LOSS" == operate_type && knots_subject_key!=""&& knots_subject_key==firstSubjectKey  {
				if len(slaveInfoMap)>0{
					if slaveInfoMap[0]["knots_subject_key"]!=nil{
						slaveKey=slaveInfoMap[0]["knots_subject_key"].(string)
						summary=slaveInfoMap[0]["summary"].(string)
					}

				}
				rebuildSlaveObjectMap["subject_key"]=slaveKey
				rebuildSlaveObjectMap["summary"]=summary
				laste_date_sql := "SELECT DATE_FORMAT(LAST_DAY('" + masterInfoMap["account_period_year"].(string) + "'),'%Y-%m-%d') AS last_date;"
				result1 := api.ExecFuncForOne(laste_date_sql, "last_date")
				masterInfoMap["account_period_year"]=result1

				for _, slave := range slaveInfoMap {
				var paramStr string
				paramsMap := make(map[string]interface{})
				// funcParamFields
				if calculate_func != "" {
					//如果执行方法不为空 执行配置中方法
					paramsMap = BuildMapFromBody(funcParamFields, masterInfoMap, paramsMap)
					paramsMap = BuildMapFromBody(funcParamFields, slave, paramsMap)
					//把对象的所有属性的值拼成字符串
					paramStr = ConcatObjectProperties(funcParamFields, paramsMap)
					list.PushBack(slave["subject_key"].(string))
					if strings.Contains(calculate_field, ",") {
						fields := strings.Split(calculate_field, ",")
						for index, item := range fields {
							calculate_func_sql_str := "select ROUND(" + calculate_func + "(" + paramStr + ",'" + strconv.Itoa(index+1) + "'" + "),2) as result;"
							result := api.ExecFuncForOne(calculate_func_sql_str, "result")
							if result==""{
								result="0"
							}
							//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
							rebuildSlaveCalMap[slave["subject_key"].(string)+"-"+item] = result
							slave[item]=result
						}
					}




				}

			}

			    //计算不同方向的累计值
			    var debitTotal float64
				var creditTotal float64
				for e := list.Front(); e != nil; e = e.Next() {
					whereOption := map[string]WhereOperation{}
					whereOption["subject_key"] = WhereOperation{
						Operation: "eq",
						Value:     e.Value,
					}
					whereOption["farm_id"] = WhereOperation{
						Operation: "eq",
						Value:     masterInfoMap["farm_id"],
					}
					querOption := QueryOption{Wheres: whereOption, Table: "farm_subject"}
					r, errorMessage:= api.Select(querOption)
					var direction string
					if errorMessage!=nil{
						fmt.Printf("errorMessage", errorMessage)
					}else{
						for _,item:=range r{
							if item["direction"]!=nil{
								direction=item["direction"].(string)
								break;
							}

						}
						fmt.Printf("rs", r)
						if direction=="2"{
							if rebuildSlaveCalMap[e.Value.(string)+"-debit_funds"].(string)!=""{
								tempTotal,error:=strconv.ParseFloat(rebuildSlaveCalMap[e.Value.(string)+"-debit_funds"].(string), 64)
								if error!=nil{
									fmt.Printf("error",error)
								}
								debitTotal=debitTotal+tempTotal
							}




						}else{
							if rebuildSlaveCalMap[e.Value.(string)+"-credit_funds"].(string)!=""{
								tempTotal,error:=strconv.ParseFloat(rebuildSlaveCalMap[e.Value.(string)+"-credit_funds"].(string), 64)
								if error!=nil{
									fmt.Printf("error",error)
								}
								creditTotal=creditTotal+tempTotal
							}


						}

					}


				}
				for _, slave := range slaveInfoMap {
					if slave["subject_key"]==slaveKey && slave["line_number"]=="1"{
						slave["credit_funds"]=debitTotal
					}
					if slave["subject_key"]==slaveKey && slave["line_number"]!="1"{
						slave["debit_funds"]=creditTotal
					}
				}

				//rebuildSlaveObjectMap["debit_funds"]=debitTotal
				//slaveInfoMap=append(slaveInfoMap,rebuildSlaveObjectMap)
				////rebuildSlaveObjectMapp=rebuildSlaveObjectMap
				//rebuildSlaveObjectMapp["debit_funds"]="0"
				//rebuildSlaveObjectMapp["summary"]=rebuildSlaveObjectMap["summary"]
				//rebuildSlaveObjectMapp["subject_key"]=rebuildSlaveObjectMap["subject_key"]
				//rebuildSlaveObjectMapp["credit_funds"]=creditTotal
				//slaveInfoMap=append(slaveInfoMap,rebuildSlaveObjectMapp)
				//slaveInfoMap=nil
				for _, slave := range slaveInfoMap {
					if slave["credit_funds"]!=nil||slave["debit_funds"]!=nil{
						//ConverStrFromMap("credit_funds",slave)
						credit_funds:=ConverStrFromMap("credit_funds",slave)
						debit_funds:=ConverStrFromMap("debit_funds",slave)
						if (credit_funds!="0"||debit_funds!="0"){
							rebuildSlaveObjectMapp=append(rebuildSlaveObjectMapp,slave)
						}

					}
				}
				if len(rebuildSlaveObjectMapp)>0{
					slaveInfoMap=rebuildSlaveObjectMapp
				}

		}



	}



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
		//if slavePriId==""{
			uuid := uuid.NewV4()
			slavePriId=uuid.String()
			slave[slavePriKey]=slavePriId
	//	}else {
		//	slave[slavePriKey]=slavePriId
		//}


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
					asyncObjectMap:=make(map[string]interface{})//构建同步数据对象


					operate_condition := operate["operate_condition"].(string)
					operate_content := operate["operate_content"].(string)

					if (operate_condition != "") {
						json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)
						conditionFileds = operateCondJsonMap["conditionFields"].(string)
						if operateCondJsonMap["conditionFieldss"]!=nil{
							conditionFileds1 = operateCondJsonMap["conditionFieldss"].(string)
						}
						if operateCondJsonMap["funcParamFields"]!=nil{
							funcParamFieldStr = operateCondJsonMap["funcParamFields"].(string)
						}

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
						if operateCondContentJsonMap["calculate_field"]!=nil{
							calculate_field=operateCondContentJsonMap["calculate_field"].(string)
						}
						if operateCondContentJsonMap["calculate_func"]!=nil{
							calculate_func=operateCondContentJsonMap["calculate_func"].(string)
						}

					}
					if operateCondContentJsonMap["operate_func"]!=nil{
						operate_func=operateCondContentJsonMap["operate_func"].(string)
					}
					//如果是 operate_type ASYNC_BATCH_SAVE 同步批量保存并计算值
					if "ASYNC_BATCH_SAVE"==operate_type{
						asyncObjectMap=BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=BuildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)
						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if calculate_func!=""{
							//如果执行方法不为空 执行配置中方法
							paramsMap=BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=BuildMapFromBody(funcParamFields,slave,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=ConcatObjectProperties(funcParamFields,paramsMap)
							calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+"),2) as result;"

							result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
							if result==""{
								result="0"
							}
							asyncObjectMap[calculate_field]=result

						}

						judgeExistsSql:="select judgeCurrentKnotsExists("+paramStr+") as id;"
						id:=api.ExecFuncForOne(judgeExistsSql,"id")
						if id==""{
							asyncObjectMap["id"]=slave["id"]
							r,errorMessage:=api.Create(operate_table,asyncObjectMap)
							fmt.Printf("r=",r,"errorMessage=",errorMessage)
						}else{//id不为空 则更新
							asyncObjectMap["id"]=id
							r,errorMessage:= api.Update(operate_table,id,asyncObjectMap)
							if errorMessage!=nil{
								fmt.Printf("errorMessage=",errorMessage)
							}
							fmt.Printf("rs=",r)

						}


					}
					// ASYNC_BATCH_SAVE_BEGIN_PEROID 计算期初
					if "ASYNC_BATCH_SAVE_BEGIN_PEROID"==operate_type{
						asyncObjectMap=BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=BuildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)

						lastYearKnotsCurrentPeriod:=make(map[string]interface{})
						lastYearKnotsCurrentYear:=make(map[string]interface{})
						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if calculate_func!=""{
							// SELECT CONCAT(DATE_FORMAT(NOW(),'%Y-%m'),'-01') as first_date;
							laste_date_sql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y-%m'),'-01') AS first_date;"
							result1:=api.ExecFuncForOne(laste_date_sql,"first_date")

							beginYearSql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y'),'-01-01') AS beginYear;"
							beginYearResult:=api.ExecFuncForOne(beginYearSql,"beginYear")

							latestYearKnotsSql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y'),'-01-31') AS beginYear;"
							latestYearKnots:=api.ExecFuncForOne(latestYearKnotsSql,"beginYear")
							//masterInfoMap["account_period_year"]=result1

							asyncObjectMap["voucher_type"]=nil
							asyncObjectMap["line_number"]=0
							asyncObjectMap["order_num"]=nil

							asyncObjectMap["summary"]="期初余额"
							asyncObjectMap["account_period_year"]=result1
							//如果执行方法不为空 执行配置中方法
							paramsMap=BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=BuildMapFromBody(funcParamFields,slave,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=ConcatObjectProperties(funcParamFields,paramsMap)



							// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
							judgeExistsSql:="select judgeCurrentBeginPeroidExists("+paramStr+",'2') as id;"
							if strings.Contains(calculate_field,","){
								fields:=strings.Split(calculate_field,",")
								for index,item:=range fields{
									calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
									result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
									//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
									if result==""{
										result="0"
									}
									asyncObjectMap[item]=result

								}
							}
							id:=api.ExecFuncForOne(judgeExistsSql,"id")

							// 判断是否需要新增上年结转记录
							judgeIsNeedCreateKnotsSql:="select judgeNeedCreateLatestKnots("+paramStr+") as id"
							lastYearKnotsId:=api.ExecFuncForOne(judgeIsNeedCreateKnotsSql,"id")
							//  	asyncObjectMap["summary"]="上年结转"
							if lastYearKnotsId==""{
								asyncObjectMap["summary"]="上年结转"
								asyncObjectMap["id"]=asyncObjectMap["id"].(string)+"-beginperoid-knots"
								asyncObjectMap["account_period_year"]=beginYearResult
								asyncObjectMap["account_period_num"]="1"
								r0,errorMessage0:=api.Create(operate_table,asyncObjectMap)
								fmt.Printf("r=",r0,"errorMessage=",errorMessage0)

								if beginYearResult!=result1{
									lastYearKnotsCurrentPeriod=asyncObjectMap
									lastYearKnotsCurrentPeriod["line_number"]=100
									lastYearKnotsCurrentPeriod["summary"]="本期合计"
									lastYearKnotsCurrentPeriod["account_period_year"]=latestYearKnots
									lastYearKnotsCurrentPeriod["account_period_num"]="1"
									lastYearKnotsCurrentPeriod["id"]=lastYearKnotsCurrentPeriod["id"].(string)+"-peroid"
									r,errorMessage:=api.Create(operate_table,lastYearKnotsCurrentPeriod)
									fmt.Printf("r=",r,"errorMessage=",errorMessage)

									lastYearKnotsCurrentYear=asyncObjectMap
									lastYearKnotsCurrentYear["line_number"]=101
									lastYearKnotsCurrentYear["summary"]="本年累计"
									lastYearKnotsCurrentYear["account_period_year"]=latestYearKnots
									lastYearKnotsCurrentYear["account_period_num"]="1"
									lastYearKnotsCurrentYear["order_num"]=0
									lastYearKnotsCurrentYear["id"]=lastYearKnotsCurrentYear["id"].(string)+"-year"
									r1,errorMessage1:=api.Create(operate_table,lastYearKnotsCurrentYear)
									fmt.Printf("r=",r1,"errorMessage=",errorMessage1)
								}

							}


							if id=="" {
								asyncObjectMap["summary"]="期初余额"
								if  lastYearKnotsId=="" && beginYearResult!=result1{
									asyncObjectMap["line_number"]=0
									asyncObjectMap["account_period_year"]=result1
									asyncObjectMap["order_num"]=0
									asyncObjectMap["id"]=asyncObjectMap["id"].(string)+"-beginperoid"
									asyncObjectMap["account_period_num"]=masterInfoMap["account_period_num"]
									r,errorMessage:=api.Create(operate_table,asyncObjectMap)
									fmt.Printf("r=",r,"errorMessage=",errorMessage)
								}

							}else{//id不为空 则更新
								asyncObjectMap["id"]=id
								asyncObjectMap["summary"]="期初余额"
								r,errorMessage:= api.Update(operate_table,id,asyncObjectMap)
								if errorMessage!=nil{
									fmt.Printf("errorMessage=",errorMessage)
								}
								fmt.Printf("rs=",r)

							}


						}


					}

					// ASYNC_BATCH_SAVE_CURRENT_PEROID 计算指定配置的值
					if "ASYNC_BATCH_SAVE_CURRENT_PEROID"==operate_type{
						asyncObjectMap=BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=BuildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)
						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if calculate_func!=""{
							// SELECT DATE_FORMAT(LAST_DAY(CURDATE()),'%Y-%m-%d') AS last_date;
							laste_date_sql:="SELECT DATE_FORMAT(LAST_DAY('"+asyncObjectMap["account_period_year"].(string)+"'),'%Y-%m-%d') AS last_date;"
							result1:=api.ExecFuncForOne(laste_date_sql,"last_date")
							//masterInfoMap["account_period_year"]=result1

							asyncObjectMap["voucher_type"]=nil
							asyncObjectMap["line_number"]=100
							asyncObjectMap["order_num"]=nil
							asyncObjectMap["summary"]="本期合计"
							asyncObjectMap["account_period_year"]=result1
							//如果执行方法不为空 执行配置中方法
							paramsMap=BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=BuildMapFromBody(funcParamFields,slave,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=ConcatObjectProperties(funcParamFields,paramsMap)



							// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
							judgeExistsSql:="select judgeCurrentPeroidExists("+paramStr+") as id;"
							if strings.Contains(calculate_field,","){
								fields:=strings.Split(calculate_field,",")
								for index,item:=range fields{
									calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
									result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
									//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
									if result==""{
										result="0"
									}
									asyncObjectMap[item]=result

								}
							}



							id:=api.ExecFuncForOne(judgeExistsSql,"id")
							if id=="" {
								asyncObjectMap["id"]=asyncObjectMap["id"].(string)+"-peroid"
								r,errorMessage:=api.Create(operate_table,asyncObjectMap)
								fmt.Printf("r=",r,"errorMessage=",errorMessage)
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
						asyncObjectMap=BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=BuildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)
						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if calculate_func!=""{
							// SELECT DATE_FORMAT(LAST_DAY(CURDATE()),'%Y-%m-%d') AS last_date;
							laste_date_sql:="SELECT DATE_FORMAT(LAST_DAY('"+asyncObjectMap["account_period_year"].(string)+"'),'%Y-%m-%d') AS last_date;"
							result1:=api.ExecFuncForOne(laste_date_sql,"last_date")
							//masterInfoMap["account_period_year"]=result1

							asyncObjectMap["voucher_type"]=nil
							asyncObjectMap["order_num"]=nil
							asyncObjectMap["line_number"]=101
							asyncObjectMap["summary"]="本年累计"
							asyncObjectMap["account_period_year"]=result1
							//如果执行方法不为空 执行配置中方法
							paramsMap=BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=BuildMapFromBody(funcParamFields,slave,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=ConcatObjectProperties(funcParamFields,paramsMap)



							// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
							judgeExistsSql:="select judgeCurrentYearExists("+paramStr+") as id;"
							if strings.Contains(calculate_field,","){
								fields:=strings.Split(calculate_field,",")
								for index,item:=range fields{
									calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
									result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
									//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
									if result==""{
										result="0"
									}
									asyncObjectMap[item]=result

								}
							}


							id:=api.ExecFuncForOne(judgeExistsSql,"id")
							if id=="" {
								asyncObjectMap["id"]=asyncObjectMap["id"].(string)+"-year"
								r,errorMessage:=api.Create(operate_table,asyncObjectMap)
								fmt.Printf("r=",r,"errorMessage=",errorMessage)
							}else{//id不为空 则更新
								asyncObjectMap["id"]=id
								r,errorMessage:= api.Update(operate_table,id,asyncObjectMap)
								if errorMessage!=nil{
									fmt.Printf("errorMessage=",errorMessage)
								}
								fmt.Printf("rs=",r)

							}



							// 判断是否需要新增上年结转记录
							judgeIsNeedCreateNextKnotsSql:="select judgeNeedCreateNextKnots("+paramStr+") as id"
							nextYearKnotsId:=api.ExecFuncForOne(judgeIsNeedCreateNextKnotsSql,"id")
							nextYearKnots:=make(map[string]interface{})
							nextYearKnotsSql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y'),'-12-31') AS beginYear;"
							nextYearKnotsResult:=api.ExecFuncForOne(nextYearKnotsSql,"beginYear")
							if nextYearKnotsId=="" && nextYearKnotsResult==result1{
								nextYearKnots=asyncObjectMap
								nextYearKnots["line_number"]=102
								nextYearKnots["summary"]="结转下年"
								nextYearKnots["account_period_year"]=nextYearKnotsResult
								nextYearKnots["id"]=nextYearKnots["id"].(string)+"-year-hnots"
								r,errorMessage:=api.Create(operate_table,nextYearKnots)
								fmt.Printf("r=",r,"errorMessage=",errorMessage)
							}


							// 判断是否需要新增上年结转记录
							judgeNeedUpdateNextKnotsSql:="select judgeNeedUpdateNextKnots("+paramStr+") as id"
							judgeNeedUpdateNextKnotsId:=api.ExecFuncForOne(judgeNeedUpdateNextKnotsSql,"id")

							if judgeNeedUpdateNextKnotsId!=""{
								nextYearKnots=asyncObjectMap
								nextYearKnots["line_number"]=102
								nextYearKnots["summary"]="结转下年"
								nextYearKnots["account_period_year"]=nextYearKnotsResult
								nextYearKnots["id"]=judgeNeedUpdateNextKnotsId
								r,errorMessage:=api.Update(operate_table,judgeNeedUpdateNextKnotsId,nextYearKnots)
								fmt.Printf("r=",r,"errorMessage=",errorMessage)
							}



						}


					}

					// ASYNC_BATCH_SAVE_SUBJECT_LEAVE
					if "ASYNC_BATCH_SAVE_SUBJECT_LEAVE"==operate_type{
						asyncObjectMap=BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=BuildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)

						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if operate_func!=""{

							//如果执行方法不为空 执行配置中方法
							paramsMap=BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=BuildMapFromBody(funcParamFields,slave,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=ConcatObjectProperties(funcParamFields,paramsMap)


							// 直接执行func 所有逻辑在func处理
							operate_func_sql:="select "+operate_func+"("+paramStr+") as result;"
							result:=api.ExecFuncForOne(operate_func_sql,"result")
							fmt.Printf("operate_func_sql-result",result)



						}


					}
					// ASYNC_BATCH_SAVE_SUBJECT_TOTAL
					if "ASYNC_BATCH_SAVE_SUBJECT_TOTAL"==operate_type{
						asyncObjectMap=BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=BuildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)

						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if operate_func!=""{

							//如果执行方法不为空 执行配置中方法
							paramsMap=BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=BuildMapFromBody(funcParamFields,slave,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=ConcatObjectProperties(funcParamFields,paramsMap)


							// 直接执行func 所有逻辑在func处理
							operate_func_sql:="select "+operate_func+"("+paramStr+") as result;"
							result:=api.ExecFuncForOne(operate_func_sql,"result")
							fmt.Printf("operate_func_sql-result",result)



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
func SelectOperaInfo(api adapter.IDatabaseAPI,tableName string,apiMethod string) (rs []map[string]interface{},errorMessage *ErrorMessage) {

	whereOption := map[string]WhereOperation{}
	whereOption["cond_table"] = WhereOperation{
		Operation: "eq",
		Value:     tableName,
	}
	whereOption["api_method"] = WhereOperation{
		Operation: "eq",
		Value:     apiMethod,
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

func ConcatObjectProperties(funcParamFields [10]string,object map[string]interface{})(string){
	var resultStr string
	b := bytes.Buffer{}
	for _,item:=range funcParamFields{
		if item!=""&&object[item]!=nil{
			switch object[item].(type) {      //多选语句switch
			case string:
				//是字符时做的事情
				b.WriteString(object[item].(string)+",")
			case float64:
				//是整数时做的事情
				b.WriteString(strconv.FormatFloat(object[item].(float64), 'f', -1, 64)+",")

			}



		}


	}
	resultStr="'"+strings.Replace(b.String(),",","','",-1)+"'"
	resultStr=strings.Replace(resultStr,",''","",-1)
	return resultStr
}
func BuildMapFromObj(fromObjec map[string]interface{},disObjec map[string]interface{})(map[string]interface{}){
	for k,v:=range fromObjec{
		disObjec[k]=v
	}
	return disObjec;
}

func BuildMapFromBody(properties [10]string,fromObjec map[string]interface{},disObjec map[string]interface{})(map[string]interface{}){
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

func (api *MysqlAPI) RelatedUpdate(operates []map[string]interface{},obj map[string]interface{}) (rowAffect int64,errorMessage *ErrorMessage) {
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
	var subjectKeyExists []map[string]interface{}
	for _,item:=range rr{
		judgeExistsFundsWhereOption0 := map[string]WhereOperation{}
		judgeExistsFundsWhereOption0["account_period_year"] = WhereOperation{
			Operation: "gte",
			Value:     masterInfoMap["account_period_year"],
		}
		judgeExistsFundsWhereOption0["account_period_num"] = WhereOperation{
			Operation: "gte",
			Value:     masterInfoMap["account_period_num"],
		}

		judgeExistsFundsWhereOption0["subject_key"] = WhereOperation{
			Operation: "eq",
			Value:     item["subject_key"],
		}

		judgeFundsQuerOption0 := QueryOption{Wheres: judgeExistsFundsWhereOption0, Table: slaveTableName+"_category_merge"}
		fundsExists0, errorMessage0:= api.Select(judgeFundsQuerOption0)
		fmt.Printf("errorMessage=",errorMessage0)
		if len(fundsExists0)>0{
			subjectKeyExists=fundsExists0
		}


		var ids []string
		var deleteEdOption QueryOption
		ids=append(ids,item["id"].(string))
		deleteEdOption.Ids=ids
		PreEvent(api,slaveTableName,"PUT",nil,deleteEdOption,"")
		_,errorMessage:=api.Delete(slaveTableName,item["id"],nil)
		fmt.Printf("errorMessage=",errorMessage)
	}

	for i, slave := range slaveInfoMap {
		whereOption:=make(map[string]WhereOperation)
		judgeExistsFundsWhereOption := map[string]WhereOperation{}
		judgeExistsFundsWhereOption["id"] = WhereOperation{
			Operation: "eq",
			Value:     slave["id"],
		}
		judgeFundsQuerOption0 := QueryOption{Wheres: judgeExistsFundsWhereOption, Table: slaveTableName}
		latestSlave, errorMessage:= api.Select(judgeFundsQuerOption0)

		judgeExistsFundsWhereOption["debit_funds"] = WhereOperation{
			Operation: "eq",
			Value:     slave["debit_funds"],
		}

		judgeFundsQuerOption := QueryOption{Wheres: judgeExistsFundsWhereOption, Table: slaveTableName}
		fundsExists, errorMessage:= api.Select(judgeFundsQuerOption)

		judgeExistsFundsWhereOption1 := map[string]WhereOperation{}
		judgeExistsFundsWhereOption1["id"] = WhereOperation{
			Operation: "eq",
			Value:     slave["id"],
		}

		judgeExistsFundsWhereOption1["credit_funds"] = WhereOperation{
			Operation: "eq",
			Value:     slave["credit_funds"],
		}

		judgeFundsQuerOption1 := QueryOption{Wheres: judgeExistsFundsWhereOption1, Table: slaveTableName}
		fundsExists1, errorMessage:= api.Select(judgeFundsQuerOption1)

		judgeExistsFundsWhereOption2 := map[string]WhereOperation{}
		judgeExistsFundsWhereOption2["id"] = WhereOperation{
			Operation: "eq",
			Value:     slave["id"],
		}

		judgeExistsFundsWhereOption2["subject_key"] = WhereOperation{
			Operation: "eq",
			Value:     slave["subject_key"],
		}

		judgeFundsQuerOption2 := QueryOption{Wheres: judgeExistsFundsWhereOption2, Table: slaveTableName}
		fundsExists2, errorMessage:= api.Select(judgeFundsQuerOption2)
		judgeExistsFundsWhereOption3 := map[string]WhereOperation{}
		judgeExistsFundsWhereOption3["account_period_year"] = WhereOperation{
			Operation: "gte",
			Value:     masterInfoMap["account_period_year"],
		}
		judgeExistsFundsWhereOption3["account_period_num"] = WhereOperation{
			Operation: "gt",
			Value:     masterInfoMap["account_period_num"],
		}

		judgeExistsFundsWhereOption3["subject_key"] = WhereOperation{
			Operation: "eq",
			Value:     slave["subject_key"],
		}

		judgeFundsQuerOption3 := QueryOption{Wheres: judgeExistsFundsWhereOption3, Table: slaveTableName+"_category_merge"}
		fundsExists3, errorMessage:= api.Select(judgeFundsQuerOption3)
		fmt.Printf("errorMessage=",errorMessage)
		if len(fundsExists3)>0{
			subjectKeyExists=fundsExists3
		}else{
			judgeExistsFundsWhereOption3["account_period_num"] = WhereOperation{
				Operation: "eq",
				Value:     masterInfoMap["account_period_num"],
			}
			judgeExistsFundsWhereOption3["order_num"] = WhereOperation{
				Operation: "gt",
				Value:     masterInfoMap["order_num"],
			}
			judgeFundsQuerOption3 := QueryOption{Wheres: judgeExistsFundsWhereOption3, Table: slaveTableName+"_category_merge"}
			fundsExists4, errorMessage:= api.Select(judgeFundsQuerOption3)
			fmt.Printf("errorMessage=",errorMessage)
			if len(fundsExists4)>0{
				subjectKeyExists=fundsExists4
			}else{
				judgeExistsFundsWhereOption3["order_num"] = WhereOperation{
					Operation: "eq",
					Value:     masterInfoMap["order_num"],
				}
				judgeExistsFundsWhereOption3["line_number"] = WhereOperation{
					Operation: "gt",
					Value:     slave["line_number"],
				}
				judgeFundsQuerOption3 := QueryOption{Wheres: judgeExistsFundsWhereOption3, Table: slaveTableName+"_category_merge"}
				fundsExists5, errorMessage:= api.Select(judgeFundsQuerOption3)
				fmt.Printf("errorMessage=",errorMessage)
				if len(fundsExists5)>0{
					subjectKeyExists=fundsExists5
				}
			}

		}


		if len(fundsExists2)<=0{
			var preOption QueryOption
			var ids []string
			for _,item:=range latestSlave{
				ids=append(ids,item["id"].(string))
			}
			preOption.Ids=ids
			PreEvent(api,slaveTableName,"PUT",nil,preOption,"")



		}
		var updateSql string
		var isNewCreatedSlaveId string
		if slave["id"]!=nil{
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
			api.RelatedCreate(operates,objCreate)
			fmt.Printf("rsCreate=",rs)
			isNewCreatedSlaveId=slave["id"].(string)
			// 新增的也y要同步计算
			// continue
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
				// 处理后置事件

				var operate_type string
				var operate_table string
				var calculate_field string
				var calculate_func string
				var conditionFileds string
				var conditionFileds1 string
				var funcParamFieldStr string
				var operateCondJsonMap map[string]interface{}
				var operateCondContentJsonMap map[string]interface{}
				var repeatCalculateData []map[string]interface{}
				var repeatCalculateData0 []map[string]interface{}
				var repeatCalculateData1 []map[string]interface{}
				var repeatCalculateData2 []map[string]interface{}
				var repeatCalculateData3 []map[string]interface{}
				var conditionFiledArr [10]string
				var conditionFiledArr1 [10]string
				//conditionFiledArr := list.New()
				//conditionFiledArr1 := list.New()
				var funcParamFields [10]string
				var operate_func string
				// 通过 OperateKey查询前置事件
				opK,errorMessage:=SelectOperaInfoByOperateKey(api,masterTableName+"-"+slaveTableName+"-PUT")
				fmt.Printf("errorMessage=",errorMessage)


// credit_funds

				if opK!=nil &&(len(fundsExists)<=0||len(fundsExists1)<=0 ||len(fundsExists2)<=0 || len(subjectKeyExists)>0){
					for _, item := range opK {
						operate_condition := item["operate_condition"].(string)
						operate_content := item["operate_content"].(string)

						if (operate_condition != "") {
							json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)

						}
						if (operate_content != "") {
							json.Unmarshal([]byte(operate_content), &operateCondContentJsonMap)
						}
						if operateCondContentJsonMap != nil {
							operate_type = operateCondContentJsonMap["operate_type"].(string)
							operate_table = operateCondContentJsonMap["operate_table"].(string)
							if operateCondContentJsonMap["calculate_func"]!=nil{
								calculate_func=operateCondContentJsonMap["calculate_func"].(string)
							}

						}


					}
					//repeatCalculateData=make([]map[string]interface{})
                    if operate_type=="CALCULATE_DEPENDY_LEAVE_FUNDS"{
                    	//先更新明细表中记录  id和详情id一样 除了合计、累计id
						sql, err := api.sql.UpdateByTableAndId(operate_table,slave["id"].(string), slave)
						if err!=nil{
							// 回滚已经插入的数据
							errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,err.Error()}
							return 0,errorMessage
						}else {
							rs, errorMessage = api.exec(sql)
							if errorMessage!=nil{
								fmt.Printf("errorMessage=",errorMessage)
							}
						}
						// 不是同一年
						whereOption["subject_key"] = WhereOperation{
							Operation: "in",
							Value:     slave["subject_key"],
						}
						whereOption["voucher_type"] = WhereOperation{
							Operation: "gt",
							Value:     "0",
						}
						whereOption["farm_id"] = WhereOperation{
							Operation: "eq",
							Value:     masterInfoMap["farm_id"],
						}
						var year string
						if masterInfoMap["account_period_year"]!=nil{
							year=masterInfoMap["account_period_year"].(string)[0:4]
						}
						whereOption["account_period_year"] = WhereOperation{
							Operation: "gt",
							Value:    year+"-12-31",
						}
						querOption := QueryOption{Wheres: whereOption, Table: operate_table}
						orders:=make(map[string]string)
						orders["N1account_period_num"]="ASC"
						orders["N2account_period_year"]="ASC"
						orders["N3order_num"]="ASC"
						orders["N4line_number"]="ASC"
						querOption.Orders=orders
						repeatCalculateData3, errorMessage= api.Select(querOption)

						// 不是同一期查询条件

						whereOption["account_period_year"] = WhereOperation{
							Operation: "like",
							Value:    year+"%",
						}
						whereOption["account_period_num"] = WhereOperation{
							Operation: "gt",
							Value:     masterInfoMap["account_period_num"],
						}
						querOption= QueryOption{Wheres: whereOption, Table: operate_table}

					//	for _,item:=range slaveInfoMap{
							slave=BuildMapFromObj(masterInfoMap,slave)
							repeatCalculateData=append(repeatCalculateData,slave)
					//	}
						repeatCalculateData0, errorMessage= api.Select(querOption)

						//是同一期的查询条件 但是不同查询凭证字号

						whereOption["account_period_num"] = WhereOperation{
							Operation: "eq",
							Value:     masterInfoMap["account_period_num"],
						}
						whereOption["order_num"] = WhereOperation{
							Operation: "gt",
							Value:     masterInfoMap["order_num"],
						}
						querOption = QueryOption{Wheres: whereOption, Table: operate_table}
						querOption.Orders=orders
						repeatCalculateData1, errorMessage= api.Select(querOption)

						//是同一期的查询条件  同一凭证字号 但是不同行号

						whereOption["account_period_num"] = WhereOperation{
							Operation: "eq",
							Value:     masterInfoMap["account_period_num"],
						}
						whereOption["order_num"] = WhereOperation{
							Operation: "eq",
							Value:     masterInfoMap["order_num"],
						}
						whereOption["line_number"] = WhereOperation{
							Operation: "gt",
							Value:     slave["line_number"],
						}
						querOption = QueryOption{Wheres: whereOption, Table: operate_table}
						querOption.Orders=orders
						repeatCalculateData2, errorMessage= api.Select(querOption)
						for _,item:=range repeatCalculateData2{
							repeatCalculateData=append(repeatCalculateData,item)
						}
						for _,item:=range repeatCalculateData1{
							repeatCalculateData=append(repeatCalculateData,item)
						}
						for _,item:=range repeatCalculateData0{
							repeatCalculateData=append(repeatCalculateData,item)
						}
						for _,item:=range repeatCalculateData3{
							repeatCalculateData=append(repeatCalculateData,item)
						}
					  fmt.Printf("repeatCalculateData=",repeatCalculateData)
					  if errorMessage!=nil{
						  fmt.Printf("errorMessage", errorMessage)
					  }else{
						  if len(fundsExists2)<=0{
							  var preOption QueryOption
							  var ids []string
							  ids=append(ids,slave["id"].(string))
							  preOption.Ids=ids

							  PreEvent(api,slaveTableName,"PUT",nil,preOption,"")
							  // latestSlave  如果修改前的科目  在同一期的历史凭证存在  需要重新计算
							  for _,item:=range latestSlave{
							  	  var optionQueryExists QueryOption

							  	   maps:=make(map[string]WhereOperation)
							  	   maps["farm_id"]=WhereOperation{
							  	   	Operation:"eq",
							  	   	Value:masterInfoMap["farm_id"],
								   }
								  maps["account_period_num"]=WhereOperation{
									  Operation:"eq",
									  Value:masterInfoMap["account_period_num"],
								  }
								     var buffer bytes.Buffer
									  buffer.WriteString(string(masterInfoMap["account_period_year"].(string)[0:4]))
									  buffer.WriteString("%")

								  maps["account_period_year"]=WhereOperation{
									  Operation:"like",
									  Value:buffer.String(),//masterInfoMap["account_period_year"],
								  }
								  maps["subject_key"]=WhereOperation{
									  Operation:"eq",
									  Value:item["subject_key"],
								  }
								  maps["voucher_type"]=WhereOperation{
									  Operation:"gt",
									  Value:"0",
								  }

								  optionQueryExists.Wheres=maps
								  optionQueryExists.Table="account_voucher_detail_category_merge"
								  rs,errorMessage:=api.Select(optionQueryExists)

								  //maps["order_num"]=WhereOperation{
									//  Operation:"eq",
									//  Value:masterInfoMap["order_num"],
								  //}
								  //maps["line_number"]=WhereOperation{
									//  Operation:"gt",
									//  Value:item["line_number"],
								  //}
								  //rs0,errorMessage:=api.Select(optionQueryExists)

								  if errorMessage!=nil{
								  	fmt.Printf("errorMessage=",errorMessage)
								  }else{
								  	for _,item:=range rs{
										repeatCalculateData=append(repeatCalculateData,item)
									}

								  }


							  }
							  slave=BuildMapFromObj(masterInfoMap,slave)

							  repeatCalculateData=append(repeatCalculateData,slave)


						  }
						  fmt.Printf("rs", rs)
					  }


				  }
					fmt.Printf("repeatCalculateData",repeatCalculateData)
				    for _,repeatItem:=range repeatCalculateData{
				  	id:=repeatItem["id"]
				  	fmt.Printf("id=",id)
				  	if isNewCreatedSlaveId==id{
				  		continue
					}
						//  删掉 本期合计 本年累计  重新计算
                      // order_num为空说明是累计数


						for _,operate:=range operates {
						asyncObjectMap:=make(map[string]interface{})//构建同步数据对象


						operate_condition := operate["operate_condition"].(string)
						operate_content := operate["operate_content"].(string)

						if (operate_condition != "") {
							json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)
							conditionFileds = operateCondJsonMap["conditionFields"].(string)
							if operateCondJsonMap["conditionFieldss"]!=nil{
								conditionFileds1 = operateCondJsonMap["conditionFieldss"].(string)
							}
							if operateCondJsonMap["funcParamFields"]!=nil{
								funcParamFieldStr = operateCondJsonMap["funcParamFields"].(string)
							}

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

							if operateCondContentJsonMap["calculate_field"]!=nil{
								calculate_field=operateCondContentJsonMap["calculate_field"].(string)
							}

							if operateCondContentJsonMap["operate_func"]!=nil{
								operate_func = operateCondContentJsonMap["operate_func"].(string)
							}
							if operateCondContentJsonMap["calculate_func"]!=nil{
								calculate_func=operateCondContentJsonMap["calculate_func"].(string)
							}
						}
							var repeatOrderNum int
							if repeatItem["order_num"]!=nil{
								repeatOrderNum,err=strconv.Atoi(repeatItem["order_num"].(string))
								if err!=nil{
									fmt.Printf("err=",err,"repeatOrderNum=",repeatOrderNum)
								}
						}

						//如果是 operate_type ASYNC_BATCH_SAVE 同步批量保存并计算值
						if "ASYNC_BATCH_SAVE"==operate_type {
							asyncObjectMap=BuildMapFromBody(conditionFiledArr,repeatItem,asyncObjectMap)
							asyncObjectMap=BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

							fmt.Printf("operate_table",operate_table)
							fmt.Printf("calculate_field",calculate_field)
							fmt.Printf("calculate_func",calculate_func)
							var paramStr string
							paramsMap:=make(map[string]interface{})
							// funcParamFields
							if calculate_func!=""{
								//如果执行方法不为空 执行配置中方法
								paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
								paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
								//把对象的所有属性的值拼成字符串
								paramStr=ConcatObjectProperties(funcParamFields,paramsMap)
								calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+"),2) as result;"

								result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
								if result==""{
									result="0"
								}
								asyncObjectMap[calculate_field]=result

							}
							var optionQueryExists QueryOption
							maps:=make(map[string]WhereOperation)
							maps["id"]=WhereOperation{
								Operation:"eq",
								Value:asyncObjectMap["id"],
							}

							optionQueryExists.Wheres=maps
							optionQueryExists.Table=operate_table

							rs,errorMessage:=api.Select(optionQueryExists)
							fmt.Printf("errorMessage=",errorMessage)
							if len(rs)>0{
								r,errorMessage:= api.Update(operate_table,asyncObjectMap["id"],asyncObjectMap)
								if errorMessage!=nil{
									fmt.Printf("errorMessage=",errorMessage)
								}
								fmt.Printf("rs=",r)
							}else{
								r,errorMessage:= api.Create(operate_table,asyncObjectMap)
								if errorMessage!=nil{
									fmt.Printf("errorMessage=",errorMessage)
								}
								fmt.Printf("rs=",r)
							}





						}

							// ASYNC_BATCH_SAVE_BEGIN_PEROID 计算期初
						 if "ASYNC_BATCH_SAVE_BEGIN_PEROID"==operate_type {
								asyncObjectMap=BuildMapFromBody(conditionFiledArr,repeatItem,asyncObjectMap)
								asyncObjectMap=BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

								fmt.Printf("operate_table",operate_table)
								fmt.Printf("calculate_field",calculate_field)
								fmt.Printf("calculate_func",calculate_func)
								var paramStr string
								paramsMap:=make(map[string]interface{})
								// funcParamFields
								if calculate_func!=""{
									// SELECT CONCAT(DATE_FORMAT(NOW(),'%Y-%m'),'-01') as first_date;
									laste_date_sql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y-%m'),'-01') AS first_date;"
									result1:=api.ExecFuncForOne(laste_date_sql,"first_date")
									//masterInfoMap["account_period_year"]=result1
									beginYearSql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y'),'-01-01') AS beginYear;"
									beginYearResult:=api.ExecFuncForOne(beginYearSql,"beginYear")
									lastDaySql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y'),'-01-31') AS lastDay;"
									lastDayResult:=api.ExecFuncForOne(lastDaySql,"lastDay")

									asyncObjectMap["voucher_type"]=nil
									asyncObjectMap["line_number"]=0
									asyncObjectMap["order_num"]=0
									asyncObjectMap["summary"]="期初余额"
									asyncObjectMap["account_period_year"]=result1
									//如果执行方法不为空 执行配置中方法
									paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
									paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
									//如果是一月份
									if result1==beginYearResult{
										asyncObjectMap["summary"]="上年结转"
									}

									//把对象的所有属性的值拼成字符串
									paramStr=ConcatObjectProperties(funcParamFields,paramsMap)



									// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
									judgeExistsSql:="select judgeCurrentBeginPeroidExists("+paramStr+",'2') as id;"
									id0:=api.ExecFuncForOne(judgeExistsSql,"id")

									judgeExistsSql1:="select judgeSubjectPeroidExists("+paramStr+") as id1;"
									id1:=api.ExecFuncForOne(judgeExistsSql1,"id1")
									if strings.Contains(calculate_field,","){
										fields:=strings.Split(calculate_field,",")
										for index,item:=range fields{
											calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
											result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
											//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
											if result==""{
												result="0"
											}
											asyncObjectMap[item]=result

										}
									}



									if id0==""{
										if id1!=""{
											asyncObjectMap["id"]=uuid.NewV4().String()+"-beginperoid"
											r,errorMessage:=api.Create(operate_table,asyncObjectMap)
											fmt.Printf("r=",r,"errorMessage=",errorMessage)
										}


									}else{//id不为空 则更新
										asyncObjectMap["id"]=id0
										r,errorMessage:= api.Update(operate_table,id0,asyncObjectMap)
										if errorMessage!=nil{
											fmt.Printf("errorMessage=",errorMessage)
										}
										fmt.Printf("rs=",r)

									}


									// 如果当期不是1 且第一期没有凭证 更新上年结转 本期合计  本年累计 judgeNeedUpdateLatestKnots
									judgeNeedUpdateLatestKnotsSql:="select judgeNeedUpdateLatestKnots("+paramStr+",'1') as id;"
									judgeNeedUpdateLatestKnotsId:=api.ExecFuncForOne(judgeNeedUpdateLatestKnotsSql,"id")
									//var latestKnotsFunds string
									if result1!=beginYearResult && judgeNeedUpdateLatestKnotsId!=""{
										asyncObjectMap["summary"]="上年结转"
										asyncObjectMap["line_number"]=0
										asyncObjectMap["order_num"]=0
										asyncObjectMap["account_period_year"]=beginYearResult
										asyncObjectMap["account_period_num"]="1"
										asyncObjectMap["id"]=judgeNeedUpdateLatestKnotsId
										asyncObjectMap=CallFunc(api,calculate_field,calculate_func,paramStr,asyncObjectMap)
										//latestKnotsFunds=asyncObjectMap["leave_funds"].(string)
										r,errorMessage:= api.Update(operate_table,judgeNeedUpdateLatestKnotsId,asyncObjectMap)
										if errorMessage!=nil{
											fmt.Printf("errorMessage=",errorMessage)
										}
										fmt.Printf("rs=",r)
									}

									// 如果当期不是1 且第一期没有凭证 更新上年结转 本期合计  本年累计 judgeNeedUpdateLatestKnots
									judgeNeedUpdateLatestKnotsSqlCureent:="select judgeNeedUpdateLatestKnots("+paramStr+",'2') as id;"
									judgeNeedUpdateLatestKnotsIdCurrent:=api.ExecFuncForOne(judgeNeedUpdateLatestKnotsSqlCureent,"id")
									if result1!=beginYearResult && judgeNeedUpdateLatestKnotsIdCurrent!=""{
										asyncObjectMap["summary"]="本期合计"
										asyncObjectMap["line_number"]=100
										asyncObjectMap["order_num"]=0
										asyncObjectMap["account_period_year"]=lastDayResult
										asyncObjectMap["account_period_num"]="1"
										asyncObjectMap["id"]=judgeNeedUpdateLatestKnotsIdCurrent
										r,errorMessage:= api.Update(operate_table,judgeNeedUpdateLatestKnotsIdCurrent,asyncObjectMap)
										if errorMessage!=nil{
											fmt.Printf("errorMessage=",errorMessage)
										}
										fmt.Printf("rs=",r)
									}

									// 如果当期不是1 且第一期没有凭证 更新上年结转 本期合计  本年累计 judgeNeedUpdateLatestKnots
									judgeNeedUpdateLatestKnotsSqlYear:="select judgeNeedUpdateLatestKnots("+paramStr+",'3') as id;"
									judgeNeedUpdateLatestKnotsIdYear:=api.ExecFuncForOne(judgeNeedUpdateLatestKnotsSqlYear,"id")
									if result1!=beginYearResult && judgeNeedUpdateLatestKnotsIdYear!=""{
										asyncObjectMap["summary"]="本年累计"
										asyncObjectMap["line_number"]=101
										asyncObjectMap["order_num"]=0
										asyncObjectMap["account_period_year"]=lastDayResult
										asyncObjectMap["account_period_num"]="1"
										asyncObjectMap["debit_funds"]="0"
										asyncObjectMap["credit_funds"]="0"
										asyncObjectMap["leave_funds"]="0"

										asyncObjectMap["id"]=judgeNeedUpdateLatestKnotsIdYear
										r,errorMessage:= api.Update(operate_table,judgeNeedUpdateLatestKnotsIdYear,asyncObjectMap)
										if errorMessage!=nil{
											fmt.Printf("errorMessage=",errorMessage)
										}
										fmt.Printf("rs=",r)
									}




								}


							}


							// ASYNC_BATCH_SAVE_CURRENT_PEROID 计算指定配置的值
						if "ASYNC_BATCH_SAVE_CURRENT_PEROID"==operate_type {
							asyncObjectMap=BuildMapFromBody(conditionFiledArr,repeatItem,asyncObjectMap)
							asyncObjectMap=BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

							fmt.Printf("operate_table",operate_table)
							fmt.Printf("calculate_field",calculate_field)
							fmt.Printf("calculate_func",calculate_func)
							var paramStr string
							paramsMap:=make(map[string]interface{})
							// funcParamFields
							if calculate_func!=""{
								// SELECT DATE_FORMAT(LAST_DAY(CURDATE()),'%Y-%m-%d') AS last_date;
								laste_date_sql:="SELECT DATE_FORMAT(LAST_DAY('"+asyncObjectMap["account_period_year"].(string)+"'),'%Y-%m-%d') AS last_date;"
								result1:=api.ExecFuncForOne(laste_date_sql,"last_date")
								//masterInfoMap["account_period_year"]=result1

								asyncObjectMap["voucher_type"]=nil
								asyncObjectMap["line_number"]=100
								asyncObjectMap["order_num"]=100
								asyncObjectMap["summary"]="本期合计"
								asyncObjectMap["account_period_year"]=result1
								//如果执行方法不为空 执行配置中方法
								paramsMap=BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
								paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
								//把对象的所有属性的值拼成字符串
								paramStr=ConcatObjectProperties(funcParamFields,paramsMap)




								if strings.Contains(calculate_field,","){
									fields:=strings.Split(calculate_field,",")
									for index,item:=range fields{
										calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
										result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
										//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
										if result==""{
											result="0"
										}
										asyncObjectMap[item]=result

									}
								}



								// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
								judgeExistsSql:="select judgeCurrentPeroidExists("+paramStr+") as id;"

								id0:=api.ExecFuncForOne(judgeExistsSql,"id")

								judgeExistsSql1:="select judgeSubjectPeroidExists("+paramStr+") as id1;"

								id1:=api.ExecFuncForOne(judgeExistsSql1,"id1")


								if id0==""{
									if id1!=""{
										asyncObjectMap["id"]=uuid.NewV4().String()+"-peroid"
										r,errorMessage:=api.Create(operate_table,asyncObjectMap)
										fmt.Printf("r=",r,"errorMessage=",errorMessage)
									}

								}else { //id不为空 则更新
									asyncObjectMap["id"] = id0
									_, errorMessage := api.Update(operate_table, id0, asyncObjectMap)
									if errorMessage != nil {
										fmt.Printf("errorMessage=", errorMessage)
									}
								}





							}


						}

						// ASYNC_BATCH_SAVE_CURRENT_YEAR
						if "ASYNC_BATCH_SAVE_CURRENT_YEAR"==operate_type{
							asyncObjectMap=BuildMapFromBody(conditionFiledArr,repeatItem,asyncObjectMap)
							asyncObjectMap=BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

							fmt.Printf("operate_table",operate_table)
							fmt.Printf("calculate_field",calculate_field)
							fmt.Printf("calculate_func",calculate_func)
							var paramStr string
							paramsMap:=make(map[string]interface{})
							// funcParamFields
							if calculate_func!=""{
								// SELECT DATE_FORMAT(LAST_DAY(CURDATE()),'%Y-%m-%d') AS last_date;
								laste_date_sql:="SELECT DATE_FORMAT(LAST_DAY('"+asyncObjectMap["account_period_year"].(string)+"'),'%Y-%m-%d') AS last_date;"
								result1:=api.ExecFuncForOne(laste_date_sql,"last_date")
								//masterInfoMap["account_period_year"]=result1

								asyncObjectMap["voucher_type"]=nil
								asyncObjectMap["order_num"]=101
								asyncObjectMap["line_number"]=101
								asyncObjectMap["summary"]="本年累计"
								asyncObjectMap["account_period_year"]=result1
								//如果执行方法不为空 执行配置中方法
								paramsMap=BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
								paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
								//把对象的所有属性的值拼成字符串
								paramStr=ConcatObjectProperties(funcParamFields,paramsMap)


								if strings.Contains(calculate_field,","){
									fields:=strings.Split(calculate_field,",")
									for index,item:=range fields{
										calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
										result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
										//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
										if result==""{
											result="0"
										}
										asyncObjectMap[item]=result

									}
								}




								// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
								judgeExistsSql:="select judgeCurrentYearExists("+paramStr+") as id;"
								id0:=api.ExecFuncForOne(judgeExistsSql,"id")
								judgeExistsSql1:="select judgeSubjectPeroidExists("+paramStr+") as id1;"
								id1:=api.ExecFuncForOne(judgeExistsSql1,"id1")
								if id0==""{
									if id1!=""{
										asyncObjectMap["id"]=uuid.NewV4().String()+"-year"
										r,errorMessage:=api.Create(operate_table,asyncObjectMap)
										fmt.Printf("r=",r,"errorMessage=",errorMessage)
									}

								}else{//id不为空 则更新
									asyncObjectMap["id"]=id0
									r,errorMessage:= api.Update(operate_table,id0,asyncObjectMap)
									if errorMessage!=nil{
										fmt.Printf("errorMessage=",errorMessage)
									}
									fmt.Printf("rs=",r)

								}



								// 判断是否需要新增上年结转记录
								judgeNeedUpdateNextKnotsSql:="select judgeNeedUpdateNextKnots("+paramStr+") as id"
								judgeNeedUpdateNextKnotsId:=api.ExecFuncForOne(judgeNeedUpdateNextKnotsSql,"id")
								nextYearKnots:=make(map[string]interface{})
								nextYearKnotsSql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y'),'-12-31') AS beginYear;"
								nextYearKnotsResult:=api.ExecFuncForOne(nextYearKnotsSql,"beginYear")
								if judgeNeedUpdateNextKnotsId!=""{
									nextYearKnots=asyncObjectMap
									nextYearKnots["line_number"]=102
									nextYearKnots["summary"]="结转下年"
									nextYearKnots["account_period_year"]=nextYearKnotsResult
									nextYearKnots["id"]=judgeNeedUpdateNextKnotsId
									r,errorMessage:=api.Update(operate_table,judgeNeedUpdateNextKnotsId,nextYearKnots)
									fmt.Printf("r=",r,"errorMessage=",errorMessage)
								}




							}


						}


						if "ASYNC_BATCH_SAVE_SUBJECT_LEAVE"==operate_type {


							asyncObjectMap=BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
							asyncObjectMap=BuildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)

							fmt.Printf("operate_table",operate_table)
							fmt.Printf("calculate_field",calculate_field)
							fmt.Printf("calculate_func",calculate_func)

							var paramStr string
							paramsMap:=make(map[string]interface{})
							// funcParamFields
							if operate_func!="" {

								//如果执行方法不为空 执行配置中方法
								paramsMap = BuildMapFromBody(funcParamFields, masterInfoMap, paramsMap)
								paramsMap = BuildMapFromBody(funcParamFields, slave, paramsMap)
								//把对象的所有属性的值拼成字符串
								paramStr = ConcatObjectProperties(funcParamFields, paramsMap)

								// 直接执行func 所有逻辑在func处理
								operate_func_sql := "select " + operate_func + "(" + paramStr + ") as result;"
								result := api.ExecFuncForOne(operate_func_sql, "result")
								fmt.Printf("operate_func_sql-result", result)

							}


							}
							// ASYNC_BATCH_SAVE_SUBJECT_TOTAL
						if "ASYNC_BATCH_SAVE_SUBJECT_TOTAL"==operate_type{
								asyncObjectMap=BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
								asyncObjectMap=BuildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)

								fmt.Printf("operate_table",operate_table)
								fmt.Printf("calculate_field",calculate_field)
								fmt.Printf("calculate_func",calculate_func)

								var paramStr string
								paramsMap:=make(map[string]interface{})
								// funcParamFields
								if operate_func!=""{

									//如果执行方法不为空 执行配置中方法
									paramsMap=BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
									paramsMap=BuildMapFromBody(funcParamFields,slave,paramsMap)
									//把对象的所有属性的值拼成字符串
									paramStr=ConcatObjectProperties(funcParamFields,paramsMap)


									// 直接执行func 所有逻辑在func处理
									operate_func_sql:="select "+operate_func+"("+paramStr+") as result;"
									result:=api.ExecFuncForOne(operate_func_sql,"result")
									fmt.Printf("operate_func_sql-result",result)



								}


							}

					}
				  }
				}



				}
			rowAaffect=rowAaffect+slaveRowAffect
		}

	}
	rowAaffect=rowAaffect+masterRowAffect
	// 异步执行任务

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
func CallFunc(api *MysqlAPI,calculate_field string,calculate_func string,paramStr string,asyncObjectMap map[string]interface{})(map[string]interface{}){
	if strings.Contains(calculate_field,","){
		fields:=strings.Split(calculate_field,",")
		for index,item:=range fields{
			calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
			result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
			//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
			if result==""{
				result="0"
			}
			asyncObjectMap[item]=result
		}
	}
	return asyncObjectMap
}