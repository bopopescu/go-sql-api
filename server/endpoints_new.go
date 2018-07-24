package server

import (
	"net/http"
	"github.com/shiyongabc/go-mysql-api/server/swagger"
	"github.com/labstack/echo"
	"github.com/shiyongabc/go-mysql-api/server/static"
	"github.com/shiyongabc/go-mysql-api/adapter"
	. "github.com/shiyongabc/go-mysql-api/types"
	. "github.com/shiyongabc/go-mysql-api/server/util"
	"math"
	"encoding/json"
	"strconv"
	"fmt"
	"strings"
	"regexp"
	"github.com/shiyongabc/go-mysql-api/server/key"
	"github.com/360EntSecGroup-Skylar/excelize"
	"os"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"github.com/garyburd/redigo/redis"

	"github.com/shiyongabc/go-mysql-api/adapter/mysql"
	//	"container/list"
	"container/list"

	"time"
//	"github.com/shiyongabc/go-mysql-api/async"
//	"context"
//	"errors"

//	"context"
	//"context"
//	"github.com/mkideal/pkg/option"
//	"context"

//	"io"
	"io"
	"bytes"
)


// mountEndpoints to echo server
func mountEndpoints(s *echo.Echo, api adapter.IDatabaseAPI,databaseName string,redisHost string) {
	s.GET("/api/"+databaseName+"/clear/cache/", endpointTableClearCacheSpecific(api,redisHost)).Name = "clear cache"

	s.POST("/api/"+databaseName+"/related/batch/", endpointRelatedBatch(api,redisHost)).Name = "batch save related table"
	s.DELETE("/api/"+databaseName+"/related/delete/", endpointRelatedDelete(api,redisHost)).Name = "batch delete related table"
	s.PUT("/api/"+databaseName+"/related/record/", endpointRelatedPatch(api)).Name = "update related table"
	s.GET("/api/"+databaseName+"/metadata/", endpointMetadata(api)).Name = "Database Metadata"
	s.POST("/api/"+databaseName+"/echo/", endpointEcho).Name = "Echo API"
	s.GET("/api/"+databaseName+"/endpoints/", endpointServerEndpoints(s)).Name = "Server Endpoints"
	s.HEAD("/api/"+databaseName+"/metadata/", endpointUpdateMetadata(api)).Name = "从DB获取最新的元数据"


	s.GET("/api/"+databaseName+"/swagger/", endpointSwaggerJSON(api)).Name = "Swagger Infomation"
	//s.GET("/api/swagger-ui.html", endpointSwaggerUI).Name = "Swagger UI"

	s.GET("/api/"+databaseName+"/:table", endpointTableGet(api,redisHost)).Name = "Retrive Some Records"
	s.POST("/api/"+databaseName+"/:table", endpointTableCreate(api,redisHost)).Name = "Create Single Record"
	s.DELETE("/api/"+databaseName+"/:table", endpointTableDelete(api,redisHost)).Name = "Remove Some Records"

	s.GET("/api/"+databaseName+"/:table/:id", endpointTableGetSpecific(api,redisHost)).Name = "Retrive Record By ID"
	s.DELETE("/api/"+databaseName+"/:table/:id", endpointTableDeleteSpecific(api,redisHost)).Name = "Delete Record By ID"
	s.PATCH("/api/"+databaseName+"/:table/:id", endpointTableUpdateSpecific(api,redisHost)).Name = "Update Record By ID"
	//  根据条件批量修改对象的局部字段
	s.PATCH("/api/"+databaseName+"/:table/where/", endpointTableUpdateSpecificField(api,redisHost)).Name = "Update Record By part field"
	s.PUT("/api/"+databaseName+"/:table/:id", endpointTableUpdateSpecific(api,redisHost)).Name = "Put Record By ID"

	s.POST("/api/"+databaseName+"/:table/batch/", endpointBatchCreate(api,redisHost)).Name = "Batch Create Records"
    //手动执行异步任务
	s.GET("/api/"+databaseName+"/async/", endpointTableAsync(api,redisHost)).Name = "exec async task"


	//创建表
	s.POST("/api/"+databaseName+"/table/", endpointTableStructorCreate(api,redisHost)).Name = "create table structure"
	//查询
	s.GET("/api/"+databaseName+"/table/", endpointGetMetadataByTable(api)).Name = "query table structure"
	//查询
	s.DELETE("/api/"+databaseName+"/table/", endpointDeleteMetadataByTable(api)).Name = "delete table structure"


	//添加列
	s.POST("/api/"+databaseName+"/table/column/", endpointTableColumnCreate(api,redisHost)).Name = "add table column"
	//修改列
	s.PUT("/api/"+databaseName+"/table/column/", endpointTableColumnPut(api,redisHost)).Name = "put table column"
	//删除列
	s.DELETE("/api/"+databaseName+"/table/column/", endpointTableColumnDelete(api,redisHost)).Name = "delete table column"

	//导入
	s.POST("/api/"+databaseName+"/import/", endpointImportData(api,redisHost)).Name = "import data to template"
	//执行func
	s.POST("/api/"+databaseName+"/func/", endpointFunc(api,redisHost)).Name = "exec function"


}

func endpointSwaggerUI(c echo.Context) error {
	return c.HTML(http.StatusOK, static.SWAGGER_UI_HTML)
}

func endpointSwaggerJSON(api adapter.IDatabaseAPI) func(c echo.Context) error {
	return func(c echo.Context) error {
		s := swagger.GenSwaggerFromDBMetadata(api.GetDatabaseMetadata())
		//s.Host = c.Request().Host
		s.Schemes = []string{c.Scheme()}
		return c.JSON(http.StatusOK, s)
	}
}

func endpointMetadata(api adapter.IDatabaseAPI) func(c echo.Context) error {
	return func(c echo.Context) error {
		return c.JSON( http.StatusOK, api.GetDatabaseMetadata())
	}
}
func endpointRelatedBatch(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		payload, errorMessage := bodyMapOf(c)
		masterTableName := payload["masterTableName"].(string)
		slaveTableName := payload["slaveTableName"].(string)
		slaveTableInfo:=payload["slaveTableInfo"].(string)
		masterTableInfo:=payload["masterTableInfo"].(string)
		slaveInfoMap,errorMessage:=mysql.JsonArr2map(slaveTableInfo)
		masterTableInfoMap,errorMessage:=mysql.Json2map(masterTableInfo)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errorMessage)
		}
		operates, errorMessage := mysql.SelectOperaInfo(api, api.GetDatabaseMetadata().DatabaseName+"."+slaveTableName, "POST")

		rowesAffected, errorMessage := api.RelatedCreate(operates,payload)
		// 后置条件处理

		var option QueryOption
		option.ExtendedArr=slaveInfoMap
		option.ExtendedMap=masterTableInfoMap
		mysql.PostEvent(api,slaveTableName,"POST",nil,option,redisHost)

		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}
		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+masterTableName+"*"
		if(redisHost!=""){
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			defer redisConn.Close()
			val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

			fmt.Println(val, err)
			//redisConn.Send("MULTI")
			for i, _ := range val {
				_, err = redisConn.Do("DEL", val[i])
				if err != nil {
					fmt.Println("redis delelte failed:", err)
				}
				fmt.Printf("DEL-CACHE",val[i], err)
			}
		}


		cacheKeyPattern1:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+slaveTableName+"*"
		if(redisHost!=""){
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			defer redisConn.Close()
			val1, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern1))

			fmt.Println(val1, err)
			//redisConn.Send("MULTI")
			for i, _ := range val1 {
				redisConn.Send("DEL", val1[i])
			}
		}


		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}
func endpointRelatedDelete(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	var count int
	return func(c echo.Context) error {
		payload, errorMessage := bodyMapOf(c)
		masterTableName:=payload["masterTableName"].(string)
		slaveTableName:=payload["slaveTableName"].(string)
		masterTableInfo:=payload["masterTableInfo"].(string)
		// isRetainMasterInfo
		isRetainMasterInfo:=payload["isRetainMasterInfo"]
		if payload["isRetainMasterInfo"]!=nil{
			isRetainMasterInfo=payload["isRetainMasterInfo"].(string)
		}else{
			isRetainMasterInfo="0"
		}

		fmt.Printf("masterTableInfo=",masterTableInfo)
		masterInfoMap:=make(map[string]interface{})
		//slaveInfoMap:=make([]map[string]interface{})

		masterInfoMap,errorMessage=mysql.Json2map(masterTableInfo)

		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		var masterIdColumnName string
		var primaryColumns []*ColumnMetadata
		primaryColumns=api.GetDatabaseMetadata().GetTableMeta(masterTableName).GetPrimaryColumns()
		for _, col := range primaryColumns {
			if col.Key == "PRI" {
				masterIdColumnName=col.ColumnName
				break;//取第一个主键
			}
		}
		//删除主表的数据
		masterId:=masterInfoMap[masterIdColumnName].(string)
		masterInfoWhereOption := map[string]WhereOperation{}
		masterInfoWhereOption[masterIdColumnName] = WhereOperation{
			Operation: "eq",
			Value:     masterId,
		}


		masterInfoQuerOption := QueryOption{Wheres: masterInfoWhereOption, Table: masterTableName}
		masterInfoMap0, errorMessage:= api.Select(masterInfoQuerOption)
		if masterInfoMap0!=nil{
			masterInfoMap=masterInfoMap0[0]
		}
		fmt.Printf("errorMessage=",errorMessage)

		if isRetainMasterInfo=="0"||isRetainMasterInfo==""{
			rs,errorMessage:=	api.Delete(masterTableName,masterId,nil)
			count=1;
			if errorMessage!=nil{
				fmt.Printf("errorMessage",errorMessage)
			}
			fmt.Printf("rs",rs)

		}



		// 删除从表数据  先查出关联的从表记录
		slaveWhere := map[string]WhereOperation{}
		slaveWhere[masterIdColumnName] = WhereOperation{
			Operation: "eq",
			Value:     masterId,
		}
		slaveOption := QueryOption{Wheres: slaveWhere, Table: slaveTableName}
		slaveInfoMap, errorMessage := api.Select(slaveOption)
		fmt.Printf("data", slaveInfoMap)
		fmt.Printf("errorMessage", errorMessage)
//保存最后一条记录
		lastSlaveWhere := map[string]WhereOperation{}
		lastSlaveWhere[masterIdColumnName] = WhereOperation{
			Operation: "eq",
			Value:     masterId,
		}
		lastSlaveWhere["voucher_type"] = WhereOperation{
			Operation: "gt",
			Value:     "0",
		}
		lastSlaveOption := QueryOption{Wheres: lastSlaveWhere, Table: "account_voucher_detail_category_merge"}
		lastSlaveInfoMap, errorMessage := api.Select(lastSlaveOption)
		fmt.Printf("lastSlaveInfoMap", lastSlaveInfoMap)
		fmt.Printf("errorMessage", errorMessage)

		var primaryColumnsSlave []*ColumnMetadata
		primaryColumnsSlave=api.GetDatabaseMetadata().GetTableMeta(slaveTableName).GetPrimaryColumns()
		var slaveColumnName string
		for _, col := range primaryColumnsSlave {
			if col.Key == "PRI" {
				slaveColumnName=col.ColumnName
				break;//取第一个主键
			}
		}
		var option QueryOption
		var ids []string
		for _,slaveInfo:=range slaveInfoMap {
			slaveId:= slaveInfo[slaveColumnName].(string)
			ids=append(ids,slaveId)
			option.Ids=ids
		}
		mysql.PreEvent(api,slaveTableName,"DELETE",nil,option,"")
		for _,slaveInfo:=range slaveInfoMap {
			slaveId:= slaveInfo[slaveColumnName].(string)
			api.Delete(slaveTableName,slaveId,nil)
			count=count+1
		}

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
		var repeatCalculateData1 []map[string]interface{}

		var conditionFiledArr [10]string
		var conditionFiledArr1 [10]string
		//conditionFiledArr := list.New()
		//conditionFiledArr1 := list.New()
		var funcParamFields [10]string
		var operate_func string
		// 通过 OperateKey查询前置事件
		opK,errorMessage:=mysql.SelectOperaInfoByOperateKey(api,masterTableName+"-"+slaveTableName+"-DELETE")
		fmt.Printf("errorMessage=",errorMessage)

		operates, errorMessage := mysql.SelectOperaInfo(api, api.GetDatabaseMetadata().DatabaseName+"."+slaveTableName, "DELETE")

		if opK!=nil{
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

				}

			}
			//repeatCalculateData=make([]map[string]interface{})
			if operate_type=="CALCULATE_DEPENDY_LEAVE_FUNDS"{
				// 查询当期删除凭证以后的所有相关科目的记录
				whereOption := map[string]WhereOperation{}
				b := bytes.Buffer{}

				//先删除记录  id和详情id一样 除了合计、累计id
				for _,slave:=range slaveInfoMap{
					b.WriteString(slave["subject_key"].(string)+",")

				}
				//  (subject_key IN ('\'40101\',\'102\'')
				inParams:="'"+strings.Replace(b.String(),",","','",-1)+"'"
				inParams=strings.Replace(inParams,",''","",-1)
				inParams=strings.Replace(inParams,"\\'","'",-1)
				inParams=strings.Replace(inParams,"''","'",-1)
				inParams=strings.Replace(inParams,"'","",-1)
				inParams=strings.Replace(inParams,",","','",-1)
				//  subject_key IN ('102\',\'501'))
// 不是同一期查询条件
				whereOption["subject_key"] = WhereOperation{
					Operation: "in",
					Value:     inParams,
				}
				whereOption["voucher_type"] = WhereOperation{
					Operation: "gt",
					Value:     "0",
				}
				whereOption["farm_id"] = WhereOperation{
					Operation: "eq",
					Value:     masterInfoMap["farm_id"],
				}
				whereOption["account_period_year"] = WhereOperation{
					Operation: "gt",
					Value:     masterInfoMap["account_period_year"],
				}
				querOption := QueryOption{Wheres: whereOption, Table: operate_table}
				orders:=make(map[string]string)
				orders["N1account_period_num"]="ASC"
				orders["N2account_period_year"]="ASC"
				orders["N3order_num"]="ASC"
				orders["N4line_number"]="ASC"
				querOption.Orders=orders
				repeatCalculateData, errorMessage= api.Select(querOption)
//是同一期的查询条件
				whereOption["account_period_year"] = WhereOperation{
					Operation: "eq",
					Value:     masterInfoMap["account_period_year"],
				}
				whereOption["account_period_num"] = WhereOperation{
					Operation: "eq",
					Value:     masterInfoMap["account_period_num"],
				}
				whereOption["order_num"] = WhereOperation{
					Operation: "gt",
					Value:     masterInfoMap["order_num"],
				}

				repeatCalculateData1, errorMessage= api.Select(querOption)
				for _,item:=range repeatCalculateData1{
					repeatCalculateData=append(repeatCalculateData,item)
				}


				if len(repeatCalculateData)<=0{
					repeatCalculateData=lastSlaveInfoMap
				}
				fmt.Printf("repeatCalculateData=",repeatCalculateData)
				if errorMessage!=nil{
					fmt.Printf("errorMessage", errorMessage)
				}else{
					fmt.Printf("rs", repeatCalculateData)
				}


			}


			for _,repeatItem:=range repeatCalculateData{
				id:=repeatItem["id"]
				fmt.Printf("repeatId=",id)
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
						if operateCondContentJsonMap["calculate_field"]!=nil {
							calculate_field=operateCondContentJsonMap["calculate_field"].(string)
						}
						if operateCondContentJsonMap["calculate_func"]!=nil{
							calculate_func=operateCondContentJsonMap["calculate_func"].(string)
						}
// operate_func
						if operateCondContentJsonMap["operate_func"]!=nil{
							operate_func=operateCondContentJsonMap["operate_func"].(string)
						}
					}

					//如果是 operate_type ASYNC_BATCH_SAVE 同步批量保存并计算值
					if "ASYNC_BATCH_SAVE"==operate_type{
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr,repeatItem,asyncObjectMap)
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)
						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if calculate_func!=""{
							//如果执行方法不为空 执行配置中方法
							paramsMap=mysql.BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
							paramsMap=mysql.BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=mysql.ConcatObjectProperties(funcParamFields,paramsMap)
							calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+"),2) as result;"

							result:=api.ExecFuncForOne(calculate_func_sql_str,"result")
							if result==""{
								result="0"
							}
							asyncObjectMap[calculate_field]=result

						}


						//judgeExistsSql:="select judgeCurrentKnotsExists("+paramStr+") as id;"
						//id:=api.ExecFuncForOne(judgeExistsSql,"id")
						//if id==""{
						//	//asyncObjectMap["id"]=repea["id"]
						//	r,errorMessage:=api.Create(operate_table,asyncObjectMap)
						//	fmt.Printf("r=",r,"errorMessage=",errorMessage)
						//}else{//id不为空 则更新
						//	asyncObjectMap["id"]=id
							r,errorMessage:= api.Update(operate_table,asyncObjectMap["id"],asyncObjectMap)
							if errorMessage!=nil{
								fmt.Printf("errorMessage=",errorMessage)
							}
							fmt.Printf("rs=",r)

						//}

					}
					// ASYNC_BATCH_SAVE_BEGIN_PEROID 计算期初
					if "ASYNC_BATCH_SAVE_BEGIN_PEROID"==operate_type{
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

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

							asyncObjectMap["voucher_type"]=nil
							asyncObjectMap["line_number"]=0
							asyncObjectMap["order_num"]=100
							asyncObjectMap["summary"]="期初余额"
							asyncObjectMap["account_period_year"]=result1
							//如果执行方法不为空 执行配置中方法
							paramsMap=mysql.BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=mysql.BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=mysql.ConcatObjectProperties(funcParamFields,paramsMap)



							// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
							judgeExistsSql:="select judgeCurrentBeginPeroidExists("+paramStr+") as id;"
							id0:=api.ExecFuncForOne(judgeExistsSql,"id")

							judgeExistsSql1:="select judgeCurrentBeginPeroidExists1("+paramStr+") as id1;"
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
									asyncObjectMap["id"]=id.(string)+"-beginperoid"
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



						}


					}


					// ASYNC_BATCH_SAVE_CURRENT_PEROID 计算指定配置的值
					if "ASYNC_BATCH_SAVE_CURRENT_PEROID"==operate_type{
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr,repeatItem,asyncObjectMap)
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

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
							paramsMap=mysql.BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=mysql.BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=mysql.ConcatObjectProperties(funcParamFields,paramsMap)
							// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
							judgeExistsSql:="select judgeCurrentPeroidExists("+paramStr+") as id;"

							id0:=api.ExecFuncForOne(judgeExistsSql,"id")

							judgeExistsSql1:="select judgeCurrentPeroidExists1("+paramStr+") as id1;"

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
									asyncObjectMap["id"]=strings.Replace(asyncObjectMap["id"].(string),"-peroid","",-1)
									asyncObjectMap["id"]=asyncObjectMap["id"].(string)+"-peroid"
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


							//}



						}


					}

					// ASYNC_BATCH_SAVE_CURRENT_YEAR
					if "ASYNC_BATCH_SAVE_CURRENT_YEAR"==operate_type{
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr,repeatItem,asyncObjectMap)
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

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
							asyncObjectMap["order_num"]=100
							asyncObjectMap["line_number"]=101
							asyncObjectMap["summary"]="本年累计"
							asyncObjectMap["account_period_year"]=result1
							//如果执行方法不为空 执行配置中方法
							paramsMap=mysql.BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=mysql.BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=mysql.ConcatObjectProperties(funcParamFields,paramsMap)
							// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
							judgeExistsSql:="select judgeCurrentYearExists("+paramStr+") as id;"
							id0:=api.ExecFuncForOne(judgeExistsSql,"id")
							judgeExistsSql1:="select judgeCurrentYearExists1("+paramStr+") as id1;"
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
									asyncObjectMap["id"]=strings.Replace(asyncObjectMap["id"].(string),"-year","",-1)
									asyncObjectMap["id"]=asyncObjectMap["id"].(string)+"-year"
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


						}


					}


					if "ASYNC_BATCH_SAVE_SUBJECT_LEAVE"==operate_type {
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)

						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if operate_func!=""{

							//如果执行方法不为空 执行配置中方法
							paramsMap=mysql.BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=mysql.BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=mysql.ConcatObjectProperties(funcParamFields,paramsMap)


							// 直接执行func 所有逻辑在func处理
							operate_func_sql:="select "+operate_func+"("+paramStr+") as result;"
							result:=api.ExecFuncForOne(operate_func_sql,"result")
							fmt.Printf("operate_func_sql-result",result)



						}


					}
					// ASYNC_BATCH_SAVE_SUBJECT_TOTAL
					if "ASYNC_BATCH_SAVE_SUBJECT_TOTAL"==operate_type{
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
						asyncObjectMap=mysql.BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

						fmt.Printf("operate_table",operate_table)
						fmt.Printf("calculate_field",calculate_field)
						fmt.Printf("calculate_func",calculate_func)

						var paramStr string
						paramsMap:=make(map[string]interface{})
						// funcParamFields
						if operate_func!=""{

							//如果执行方法不为空 执行配置中方法
							paramsMap=mysql.BuildMapFromBody(funcParamFields,masterInfoMap,paramsMap)
							paramsMap=mysql.BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
							//把对象的所有属性的值拼成字符串
							paramStr=mysql.ConcatObjectProperties(funcParamFields,paramsMap)


							// 直接执行func 所有逻辑在func处理
							operate_func_sql:="select "+operate_func+"("+paramStr+") as result;"
							result:=api.ExecFuncForOne(operate_func_sql,"result")
							fmt.Printf("operate_func_sql-result",result)



						}


					}

				}
			}
		}
		

		var arr []map[string]interface{}
		arr=append(arr,payload)
		option.ExtendedArr=arr

		option.ExtendedMap=masterInfoMap

		// 后置事件
		mysql.PostEvent(api,slaveTableName,"DELETE",nil,option,"")

		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}
		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+masterTableName+"*"
		if(redisHost!=""){
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			defer redisConn.Close()
			val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

			fmt.Println(val, err)
			//redisConn.Send("MULTI")
			for i, _ := range val {
				_, err = redisConn.Do("DEL", val[i])
				if err != nil {
					fmt.Println("redis delelte failed:", err)
				}
				fmt.Printf("DEL-CACHE",val[i], err)
			}
		}


		cacheKeyPattern1:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+slaveTableName+"*"
		if(redisHost!=""){
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			defer redisConn.Close()
			val1, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern1))

			fmt.Println(val1, err)
			//redisConn.Send("MULTI")
			for i, _ := range val1 {
				redisConn.Send("DEL", val1[i])
			}
		}


		return c.String(http.StatusOK, strconv.Itoa(count))
	}
}

func endpointRelatedPatch(api adapter.IDatabaseAPI) func(c echo.Context) error {
	return func(c echo.Context) error {
		payload, errorMessage := bodyMapOf(c)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		var slaveTableName string
		if payload["slaveTableName"]!=nil{
			slaveTableName=payload["slaveTableName"].(string)
		}

		operates, errorMessage := mysql.SelectOperaInfo(api, api.GetDatabaseMetadata().DatabaseName+"."+slaveTableName, "PUT")
		rowesAffected, errorMessage := api.RelatedUpdate(operates, payload)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}

		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointEcho(c echo.Context) (err error) {
	contentType:=c.Request().Header.Get("Content-Type")
	if(contentType==""){
		contentType="text/plain"
	}
	return c.Stream(http.StatusOK,contentType ,c.Request().Body)
}

func endpointUpdateMetadata(api adapter.IDatabaseAPI) func(c echo.Context) error {
	return func(c echo.Context) error {
		api.UpdateAPIMetadata()
		return c.String(http.StatusOK, strconv.Itoa(1))
	}
}
// endpointGetMetadataByTable

func endpointGetMetadataByTable(api adapter.IDatabaseAPI) func(c echo.Context) error {
	return func(c echo.Context) error {
		//api.GetDatabaseMetadata().GetTableMeta()
		tableName:=c.QueryParam(key.TABLE_NAME)
		tableMetadata:=	api.GetDatabaseTableMetadata(tableName)

		return c.JSON(http.StatusOK, tableMetadata )
	}
}

func endpointTableGet(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	fmt.Printf("startTime=",time.Now())
	return func(c echo.Context) error {
		tableName := c.Param("table")
		option ,errorMessage:= parseQueryParams(c)
		option.Table = tableName
		// 如果是查询商品列表 隔离绿通公司查询商品
		// 如果没有传服务商id  则默认查 绿通公司的商品
		if tableName=="goods_info_view" || tableName=="goods_category"{
			if  option.Wheres[tableName+".dis_service_id"].Operation=="" {
				if option.Wheres==nil{
					option.Wheres=map[string]WhereOperation{}
				}
				option.Wheres[tableName+".dis_service_id"]=WhereOperation{
					Operation:"eq",
					Value:"a505f58f-6cdd-41af-93c8-9eddffcb993b",
				}
			}
		}
			paramBytes,err:=option.MarshalJSON()
		if err!=nil{
			fmt.Printf("err",err)
		}

		orderBytes,err:=json.Marshal(option.Orders)
		if err!=nil{
			fmt.Printf("err",err)
		}
		// 如果是查询虚拟子表的所有字段
		var fields []string
		if option.FieldsType=="1"{
			wMapHeadContent := map[string]WhereOperation{}
			wMapHeadContent["template_key"] = WhereOperation{
				Operation: "eq",
				Value:     strings.Replace(tableName,"_report_detail","_template",-1),
			}
			wMapHeadContent["is_slave_field"] = WhereOperation{
				Operation: "eq",
				Value:     "1",
			}
			optionHeadContent := QueryOption{Wheres: wMapHeadContent, Table: "export_template_detail"}
			order:=make(map[string]string)
			order["j"]="asc"
			optionHeadContent.Orders=order
			headContent, errorMessage := api.Select(optionHeadContent)
			fmt.Printf("dataContent", headContent)
			fmt.Printf("errorMessage", errorMessage)


			for _,item:=range headContent{
				fields=append(fields,item["column_name"].(string))
			}
			//option.Fields=fields

		}
		orderParam:=string(orderBytes[:])
		params:=string(paramBytes[:])
		params=params+orderParam
		params=strings.Replace(params,"\"","-",-1)
		params=strings.Replace(params,":","-",-1)
		params=strings.Replace(params,",","-",-1)
		params=strings.Replace(params,"{","",-1)
		params=strings.Replace(params,"}","",-1)
		params=strings.Replace(params,"-","",-1)
		params=strings.Replace(params,"null","",-1)
		params=strings.Replace(params,"[]","",-1)
		params=strings.Replace(params,"Table","",-1)
		params=strings.Replace(params,"Index","",-1)
		params=strings.Replace(params,"Limit","",-1)
		params=strings.Replace(params,"Offset","",-1)
		params=strings.Replace(params,"Fields","",-1)
		params=strings.Replace(params,"FieldsType","",-1)
		params=strings.Replace(params,"Links","",-1)
		params=strings.Replace(params,"Wheres","",-1)
		params=strings.Replace(params,"Search","",-1)
		params=strings.Replace(params,"\n","",-1)
		params=strings.Replace(params," ","",-1)
		params=strings.Replace(params,"%","",-1)
		params=strings.Replace(params,".","",-1)
//params=option.Orders

		params="/api/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"/"+params
		fmt.Printf("params=",params)
		var cacheData string

		// 先从配置中获取是否需要缓存

		whereOption := map[string]WhereOperation{}
			whereOption["view_name"] = WhereOperation{
				Operation: "eq",
				Value:     tableName,
			}
		viewQuerOption := QueryOption{Wheres: whereOption, Table: "view_config"}
		rsQuery, errorMessage:= api.Select(viewQuerOption)
		if errorMessage!=nil{
			fmt.Printf("errorMessage", errorMessage)
		}else{
			fmt.Printf("rs", rsQuery)
		}
       // is_need_cache
       var isNeedCache int
		var isNeedPostEvent int
       // 返回的字段是否需要计算公式计算

       for _,rsq:=range rsQuery{
		   isNeedCacheStr:=rsq["is_need_cache"].(string)
		   isNeedPostEventStr:=rsq["is_need_post_event"].(string)
		   isNeedCache,err=strconv.Atoi(isNeedCacheStr)
		   isNeedPostEvent,err=strconv.Atoi(isNeedPostEventStr)
	   }

		if isNeedCache==1&&redisHost!=""{
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			cacheData, err = redis.String(redisConn.Do("GET", params))

			if err != nil {
				fmt.Println("redis get failed:", err)
			} else {
				fmt.Printf("Get mykey: %v \n", cacheData)
			}
		}

		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}


		if option.Index==0{
			// 如果缓存中有值 用缓存中的值  否则把查询出来的值放在缓存中
			if cacheData!="QUEUED"&&cacheData!=""&&cacheData!="null"{
				return responseTableGet(c,cacheData,false,tableName,api,params,redisHost,isNeedCache,option)
			}

			//无需分页,直接返回数组
			data, errorMessage := api.Select(option)
			// 如果有虚拟子表 把子表内容  1 支持虚拟子表字段  2 查所有
			if option.FieldsType=="2" || option.FieldsType=="1"{
				data=obtainSubVirtualData(api,tableName,option.Wheres["account_period_year"].Value,data,option.FieldsType)
			}





			// 无分页的后置事件
			if isNeedPostEvent==1{
				mysql.PostEvent(api,tableName,"GET",data,option,redisHost)
			}
			if errorMessage != nil {
				return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
			}
			return responseTableGet(c,data,false,tableName,api,params,redisHost,isNeedCache,option)
		}else{
			var cacheTotalCount string
			if(isNeedCache==1&&redisHost!=""){
				pool:=newPool(redisHost)
				redisConn:=pool.Get()
				defer redisConn.Close()
				cacheTotalCount,err=redis.String(redisConn.Do("GET",params+"-totalCount"))

			}
			//cacheTotalCount=cacheTotalCount.(string)
			fmt.Printf("cacheTotalCount",cacheTotalCount)
			fmt.Printf("err",err)
			fmt.Printf("cacheData",cacheData)
			if cacheTotalCount!="" &&cacheData!="QUEUED"&&cacheData!=""&&cacheData!="null"&&err==nil{
				totalCount:=0
				totalCount,err:=strconv.Atoi(cacheTotalCount)
				if err!=nil{
					fmt.Printf("err",err)
				}
				return responseTableGet(c, &Paginator{int(option.Offset/option.Limit+1),option.Limit, int(math.Ceil(float64(totalCount)/float64(option.Limit))),totalCount,cacheData},true,tableName,api,params,redisHost,isNeedCache,option)

			}else{

				//分页
				totalCount,errorMessage:=api.SelectTotalCount(option)
				if errorMessage != nil {
					return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
				}

				data, errorMessage := api.Select(option)
				if(isNeedCache==1&&redisHost!=""){
					pool:=newPool(redisHost)
					redisConn:=pool.Get()
					defer redisConn.Close()
					redisConn.Do("SET",params+"-totalCount",totalCount)
				}

				if errorMessage != nil {
					return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
				}
				return responseTableGet(c, &Paginator{int(option.Offset/option.Limit+1),option.Limit, int(math.Ceil(float64(totalCount)/float64(option.Limit))),totalCount,data},true,tableName,api,params,redisHost,isNeedCache,option)

			}


		}
	}
}

func obtainSubVirtualData(api adapter.IDatabaseAPI,tableName string,accountPeroidYear interface{},data []map[string]interface{},isQuerySlaves string)([]map[string]interface{}){
	var tempMap []map[string]interface{}
	var optionSub QueryOption
	subWheres:=make(map[string]WhereOperation)
	orders:=make(map[string]string)
	optionSub.Table="sub_report_config"
	//optionC.Fields=[]string{caculateFromFiled}
	subVirtualName:=tableName
	subVirtualName=strings.Replace(subVirtualName,"_report_detail","_template",-1)
	subWheres["template_key"] = WhereOperation{
		Operation: "eq",
		Value: subVirtualName    ,
	}
	//  account_period_year
	if accountPeroidYear!=nil{
		subWheres["account_period_year"] = WhereOperation{
			Operation: "eq",
			Value: accountPeroidYear    ,
		}
	}

	//optionSub.Wheres=subWheres
	orders["order_num"]="asc"
	optionSub.Orders=orders

		for _,item:=range data{
				// template_detail_id
				if item["id"]!=""{
					subWheres["template_detail_id"] = WhereOperation{
						Operation: "eq",
						Value: item["id"]    ,
					}
				}
			optionSub.Wheres=subWheres
				subData, errorMessage := api.Select(optionSub)
				b := bytes.Buffer{}
				var rs []map[string]interface{}
				if errorMessage!=nil{
					fmt.Printf("errorMessage=",errorMessage)
				}else if len(subData)>0{
					var subSqlStr string
					b.WriteString("select ")
					for index,item:=range subData{
						column_name:=item["column_name"].(string)
						column_value:=item["column_value"].(string)
						if index<len(subData)-1{
							b.WriteString(column_value+" as "+column_name+",")
						}else{
							b.WriteString(column_value+" as "+column_name)
						}
					}
					subSqlStr=b.String()
					rs,errorMessage=api.ExecFunc(subSqlStr)
				}
                 tempItem:=make(map[string]interface{})
				for _,subItem:=range subData{
					columnName:=subItem["column_name"].(string)
					item[columnName]=rs[0][columnName]
					tempItem[columnName]=rs[0][columnName]
				}
			   tempMap=append(tempMap,tempItem)
			}
if isQuerySlaves=="1"{
	return tempMap
}

return data
}

func asyncFunc(x,y int,c chan int){
	fmt.Printf("async-test0",time.Now())
	// 模拟异步处理耗费的时间
	time.Sleep(5*time.Second)
	fmt.Printf("async-test1",time.Now())
	// 向管道传值
	c <- x + y
}
func asyncCalculete(api adapter.IDatabaseAPI,where string,asyncKey string,c chan int){
	fmt.Printf("async-test0",time.Now())
	// 模拟异步处理耗费的时间
	//time.Sleep(5*time.Second)

	option ,errorMessage:= parseWhereParams(where)

	// 根据key查询操作配置
	operates,errorMessage:=	mysql.SelectOperaInfoByAsyncKey(api,asyncKey)
	var operate_condition string
	var operateConditionJsonMap map[string]interface{}
	var operateContentJsonMap map[string]interface{}
	var conditionFieldKey string
	var operate_content string
	var operate_type string
	var operate_report_type string
	var action_type string
	var conditionFiledArr [5]string

	// report_diy_table_info:="report_diy_info"
	 report_diy_table_cell:="report_diy_cells"
	report_diy_table_cell_value:="report_diy_cells_value"
	for _,operate:=range operates {
		operate_content = operate["operate_content"].(string)
		operate_condition = operate["operate_condition"].(string)
	}
	fmt.Printf("option=",option,",errorMessage=",errorMessage)
	if (operate_condition != "") {
		json.Unmarshal([]byte(operate_condition), &operateConditionJsonMap)
	}
	if (operate_content != "") {
		json.Unmarshal([]byte(operate_content), &operateContentJsonMap)
	}
	if operateConditionJsonMap!=nil{
		conditionFieldKey = operateConditionJsonMap["conditionFieldKey"].(string)
		fmt.Printf("conditionFieldKey",conditionFieldKey)
		// operate_report_type
		conditionFileds:=operateConditionJsonMap["conditionFields"].(string)
		json.Unmarshal([]byte(conditionFileds), &conditionFiledArr)
	}
	if operateContentJsonMap!=nil{
		if operateContentJsonMap["operate_report_type"]!=nil{
			operate_report_type=operateContentJsonMap["operate_report_type"].(string)
		}

	}

	var operateCondContentJsonMap map[string]interface{}
	if (operate_content != "") {
		json.Unmarshal([]byte(operate_content), &operateCondContentJsonMap)
	}
	if operateCondContentJsonMap!=nil{
		operate_type = operateCondContentJsonMap["operate_type"].(string)
		action_type = operateCondContentJsonMap["action_type"].(string)
	}





	if operate_type!="" && operate_type=="RELATED_QUERY"{
		// DEPENDY_CACULATE
		if action_type!="" && action_type=="DEPENDY_CACULATE"{
			var optionC QueryOption

			optionC.Table=report_diy_table_cell
			whereCell:=make(map[string]WhereOperation)
			if operate_report_type!=""{
				whereCell["report_type"] = WhereOperation{
					Operation: "eq",
					Value:     operate_report_type,
				}
			}

			optionC.Wheres=whereCell
			orders:=make(map[string]string)
			orders["N1row"]="asc"
			orders["N2col"]="asc"
			optionC.Orders=orders
			var dataC []map[string]interface{}
			dataC, errorMessage= api.Select(optionC)
			fmt.Printf("dataC",dataC)
			//  如果datac 没有值 查询上期 直到有值为止
		//	period_num, err := strconv.Atoi(option.Wheres["account_period_num"].Value.(string))
		//	fmt.Printf("err=",err)
			//if len(dataC)<=0{
			//	wheres["account_period_num"] = WhereOperation{
			//		Operation: "eq",
			//		Value:    period_num-1,
			//	}
			//	optionC.Wheres=wheres
			//	dataC, errorMessage = api.Select(optionC)
			//
			//}
			//查询公共条件
			var wheresExp map[string]WhereOperation
			wheresExp=make(map[string]WhereOperation)
			for _,item:=range conditionFiledArr{
				if strings.Contains(item,"="){
					arr:=strings.Split(item,"=")
					wheresExp[arr[0]] = WhereOperation{
						Operation:"eq",
						Value:     arr[1],// 如果是like类型Operation替换掉%
					}
				}
				if item!=""&&option.Wheres[report_diy_table_cell_value+"."+item].Value!=nil {
					wheresExp[item] = WhereOperation{
						Operation:option.Wheres[report_diy_table_cell_value+"."+item].Operation,
						Value:     option.Wheres[report_diy_table_cell_value+"."+item].Value.(string),// 如果是like类型Operation替换掉%
					}
				}

			}
			var lineValueMap map[string]float64
			lineValueMap=make(map[string]float64)
			var dataTempArr []map[string]interface{}
			var caculateValue string
			if errorMessage==nil{
				//计算每一项值 不包括总值
				for _,datac:=range dataC {
					dataTemp:=make(map[string]interface{})
					// 把单元格的坐标作为计算合计的key
					var rowStr string
					var colStr string
					switch datac["row"].(type) {
					case string:
						rowStr=datac["row"].(string)
					case int:
						rowStr=strconv.Itoa(datac["row"].(int))
					}
					switch datac["col"].(type) {
					case string:
						colStr=datac["col"].(string)
					case int:
						colStr=strconv.Itoa(datac["col"].(int))
					}
					cellKey:="cell"+rowStr+colStr

					datac["create_time"]=time.Now().Format("2006-01-02 15:04:05")
					dataTemp=datac
					for _,item:=range conditionFiledArr{
						if item!=""&&option.Wheres[report_diy_table_cell_value+"."+item].Value!=nil {
							itemValue:=option.Wheres[report_diy_table_cell_value+"."+item].Value.(string)
							itemValue=strings.Replace(itemValue,"%","",-1)
							dataTemp[item]=itemValue

						}

					}
					if datac["value"] !=nil{//获取表达式
						switch datac["value"].(type) {
						case string:
							caculateValue=datac["value"].(string)

						}
					}

					if caculateValue!=""{
						if !strings.Contains(caculateValue,"="){
							dataTempArr=append(dataTempArr,dataTemp)
							continue
						}

						arr:=strings.Split(caculateValue,"=")

						if len(arr)>=2{
							//lineNumber=arr[0]
							caculateValue=arr[1]
						}
						calResult,errorMessage:=calculateByExpressStr(api,conditionFieldKey,wheresExp,caculateValue)
						fmt.Printf("errorMessage=",errorMessage)




						dataTempArr=append(dataTempArr,dataTemp)
						caculateExpressR := regexp.MustCompile("([\\w]+)\\.([\\w]+)\\.([\\d]+)")
						caculateExpressRb:=caculateExpressR.MatchString(caculateValue)

						if caculateExpressRb{
							datac["value"]=strconv.FormatFloat(calResult, 'f', 2, 64)
							dataTemp["value"]=calResult
						}
						if cellKey!=""&&caculateExpressRb{
							// 当期
							lineValueMap[cellKey]=calResult
						}

					//	rs,errormessge:=api.Update(report_diy_table_cell,datac["id"],datac)
					//	fmt.Printf("rs=",rs,"errormessge=",errormessge)

					}


				}
				//计算每一项的总值
				for _,datac:=range dataC {
					// 把单元格的坐标作为计算合计的key
					reportType:=datac["report_type"].(string)
					var colStr string
					var rowStr string
					var cellKey string
					var aCellKey string
					var bCellKey string
					switch datac["col"].(type) {
					case string:
						colStr=datac["col"].(string)
					case int:
						colStr=strconv.Itoa(datac["col"].(int))
					}
					switch datac["row"].(type) {
					case string:
						rowStr=datac["row"].(string)
					case int:
						rowStr=strconv.Itoa(datac["row"].(int))
					}
					cellKey="cell"+rowStr+colStr
					//caculateValue="11=account_subject_left_view.current_credit_funds.321"
					//caculateValue="1=account_subject_left_view.end_debit_funds.101+account_subject_left_view.end_debit_funds.102"
					//caculateValue="123+account_subject_left_view.begin_debit_funds.102"
					//caculateValue="6=1+2-3-4-5"
					//caculateValue="10=9+8"
					//caculateValue="9=6+7-8"
					//caculateValue="6=1+2-3-4-5"
					//caculateValue="064c92ac-31a7-11e8-9d9b-0242ac110002"
					//r := regexp.MustCompile("\\'(.*?)\\'\\.([\\w]+)\\((.*?)\\)")

					datac["create_time"]=time.Now().Format("2006-01-02 15:04:05")
					if datac["value"] !=nil{//获取表达式
						switch datac["value"].(type) {
						case string:
							caculateValue=datac["value"].(string)

						}

					}

					if caculateValue!=""{

						if !strings.Contains(caculateValue,"="){
							continue
						}

						arr:=strings.Split(caculateValue,"=")
						//var lineNumber string
						if len(arr)>=2{
							//lineNumber=arr[0]
							caculateValue=arr[1]
							//fmt.Printf("lineNumber=",lineNumber)
						}

						//numberR := regexp.MustCompile("(^[\\d]+)$")

						totalExpressR := regexp.MustCompile("^([\\d]+[.\\]?[\\d]{0,})([\\+|\\-]?)([\\d]{0,}[.\\]?[\\d]{0,})")
						// UUID 匹配
						totalExpressR1 := regexp.MustCompile("^([0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})$")
						//numberRb:=numberR.MatchString(caculateValue)

						totalExpressRb:=totalExpressR.MatchString(caculateValue)// 064c92ac-31a7-11e8-9d9b-0242ac110002 true
						totalExpressRb1:=totalExpressR1.MatchString(caculateValue)
						//fmt.Printf(" caculateExpressRb=",caculateExpressRb," totalExpressRb=",totalExpressRb," totalExpressRb1=",totalExpressRb1)


						if  totalExpressRb&&!totalExpressRb1{
							var isFirst=true
							for{
								if totalExpressRb&&!totalExpressRb1{
									// 总值计算表达式 begin_period_total=9+8
									//= "8+9"
									//= "8"
									//= "+"
									//= "9"
									arr := totalExpressR.FindStringSubmatch(caculateValue)
									fmt.Printf("arr=",arr)
									itemValue:=arr[0]
									a:=arr[1]
									operate:=arr[2]
									if operate==""{
										operate="+"
									}
									b:=arr[3]
									var af float64
									var bf float64
									//根据 定义的line_number查询单元格坐标
									aRowStr,errorMessage:=mysql.ObtainDefineLocal(api,reportType,a)
									fmt.Printf("errorMessage=",errorMessage)
									aCellKey="cell"+aRowStr+colStr
									af=lineValueMap[aCellKey]
									if !isFirst{
										resultF,error:=strconv.ParseFloat(a, 64)
										if error!=nil{
											fmt.Printf("error=",error)
										}else{
											af=resultF
										}
									}
									var bRowStr string
									if b!=""{
										bRowStr,errorMessage=mysql.ObtainDefineLocal(api,reportType,b)
									}

									fmt.Printf("errorMessage=",errorMessage)
									bCellKey="cell"+bRowStr+colStr
									bf=lineValueMap[bCellKey]
									calResult:=Calc(operate,af,bf)
									resultStr:=strconv.FormatFloat(calResult, 'f', 2, 64)
									//if itemValue==resultStr
									caculateValue=	strings.Replace(caculateValue,itemValue,resultStr,-1)
									if caculateValue=="0"{
										caculateValue=""
									}
									totalExpressRb=totalExpressR.MatchString(caculateValue)
									totalExpressRb1=totalExpressR1.MatchString(caculateValue)
									isFirst=false
									if itemValue==resultStr||(!totalExpressRb){
										 dataTemp:=make(map[string]interface{})

										datac["create_time"]=time.Now().Format("2006-01-02 15:04:05")
										dataTemp=datac
										for _,item:=range conditionFiledArr{
											if item!=""&&option.Wheres[report_diy_table_cell_value+"."+item].Value!=nil {
												itemValue:=option.Wheres[report_diy_table_cell_value+"."+item].Value.(string)
												itemValue=strings.Replace(itemValue,"%","",-1)
												dataTemp[item]=itemValue

											}

										}

										datac["value"]=calResult
										dataTemp["value"]=calResult
										dataTemp["id"]=uuid.NewV4().String()

										dataTempArr=append(dataTempArr,dataTemp)
										if  cellKey!=""{
											// 当期
											lineValueMap[cellKey]=calResult
										}
										totalExpressRb=false
									}
									//  arr=%!(EXTRA []string=[601 601  ])
									//go func() {
									//	fmt.Printf("shiyongabc")
									//	time.Sleep(time.Second)
									//}()


								}else{
									break
								}
							}


						}
					}

				}
				// 批量插入计算结果
				// 先判断是否存在
				var isExistsWhere map[string]WhereOperation
				isExistsWhere=option.Wheres
				isExistsWhere["report_type"]=WhereOperation{
					Operation:"eq",
					Value:operate_report_type,
				}
				option.Wheres=isExistsWhere
				option.Table=report_diy_table_cell_value
				rs, errorMessage:= api.Select(option)
				if errorMessage!=nil{
					fmt.Printf("errorMessage=",errorMessage)
					return;
				}else if len(rs)>0{
					for _,item:=range rs{
						_,errorMessage:=api.Delete(report_diy_table_cell_value,item["id"],nil)
						fmt.Printf("delete-errorMessage:",errorMessage)
					}
				}

				for _,item:=range dataTempArr{
					item["id"]=uuid.NewV4().String()
					_, errorMessage:=api.Create(report_diy_table_cell_value,item)
					fmt.Printf("create-error-errorMessage:",errorMessage)
				}

			}



		}
	}

	// 添加汇总报表监控信息  report_diy_info
	var existsOption QueryOption
	 existsWhere:=make(map[string]WhereOperation)
	existsOption.Table="report_diy_info"
	existsWhere["report_type"]=WhereOperation{
		Operation:"eq",
		Value:asyncKey,
	}
	existsWhere["report_title"]=WhereOperation{
		Operation:"like",
		Value:"汇总%",
	}
	existsOption.Wheres=existsWhere
	rs,errorMessage:=api.Select(existsOption)
	if len(rs)>0{
		var existsMonitorOption QueryOption
		 existsMonitorWhere:=make(map[string]WhereOperation)
		 var farmId string
		 if option.Wheres[report_diy_table_cell_value+"."+"farm_id"].Value!=nil{
		 	farmId=option.Wheres[report_diy_table_cell_value+"."+"farm_id"].Value.(string)
		 }
		 existsMonitorWhere["farm_id"]=WhereOperation{
			Operation:"eq",
			Value:farmId,
		}
		var accountYear string
		if option.Wheres[report_diy_table_cell_value+"."+"account_period_year"].Value!=nil{
			accountYear=option.Wheres[report_diy_table_cell_value+"."+"account_period_year"].Value.(string)
			accountYear=strings.Replace(accountYear,"%","",-1)
		}
		existsMonitorWhere["account_year"]=WhereOperation{
			Operation:"eq",
			Value:accountYear,
		}
		var quarter int
		if option.Wheres[report_diy_table_cell_value+"."+"account_period_num"].Value!=nil{
			peroidNum:=option.Wheres[report_diy_table_cell_value+"."+"account_period_num"].Value.(string)
			quarter=ObtainQuarter(peroidNum)
		}
		existsMonitorWhere["quarter"]=WhereOperation{
			Operation:"eq",
			Value:quarter,
		}
		existsMonitorOption.Table="report_monitor"
		existsMonitorOption.Wheres=existsMonitorWhere
		data,errorMessage:= api.Select(existsMonitorOption)
		fmt.Printf("errorMessage=",errorMessage)
		var timeOutDays string
		for _,item:=range data{
			id:=item["id"].(string)
			if item["timeout_days"]!=nil{
				timeOutDays=item["timeout_days"].(string)
			}

			api.Delete("report_monitor",id,nil)

		}

		monitorMap:=make(map[string]interface{})
		monitorMap["id"]=uuid.NewV4().String()
		monitorMap["create_time"]=time.Now().Format("2006-01-02 15:04:05")

		monitorMap["farm_id"]=farmId
		monitorMap["account_year"]=accountYear
		monitorMap["quarter"]=quarter
		monitorMap["report_status"]="1"
		if timeOutDays!=""&& timeOutDays!="0"{
			monitorMap["report_status"]="2"
		}
		monitorMap["is_use_account_platform"]="1"
		_,errorMessage=api.Create("report_monitor",monitorMap)
		fmt.Printf("errorMessage=",errorMessage)
	}
	fmt.Printf("async-test1",time.Now())
	// 向管道传值
	c <- 1
}
// 根据表达式字符串计算值
func calculateByExpressStr(api adapter.IDatabaseAPI,conditionFiledKey string,wheres map[string]WhereOperation,caculateValue string)(calResult float64,errorMessage *ErrorMessage){
	//caculateValue="4=3"
	arr:=strings.Split(caculateValue,"=")
	var lineNumber string
	if len(arr)>=2{
		lineNumber=arr[0]
		caculateValue=arr[1]
		fmt.Printf("lineNumber=",lineNumber)
	}
	//caculateValue="11=account_subject_left_view.current_credit_funds.321.pre"
	//caculateValue="1=account_subject_left_view.end_debit_funds.101+account_subject_left_view.end_debit_funds.102"
	//caculateValue="123+account_subject_left_view.begin_debit_funds.102"
	//caculateValue="6=1+2-3-4-5"
	//caculateValue="10=9+8"
	//caculateValue="9=6+7-8"
	//caculateValue="6=1+2-3-4-5"
	//caculateValue="064c92ac-31a7-11e8-9d9b-0242ac110002"
	//r := regexp.MustCompile("\\'(.*?)\\'\\.([\\w]+)\\((.*?)\\)")
	//numberR := regexp.MustCompile("(^[\\d]+)$")
	caculateExpressR := regexp.MustCompile("([\\w]+)\\.([\\w]+)\\.([\\d]+(\\.pre)?)")
	//totalExpressR := regexp.MustCompile("^([\\d]+[.\\]?[\\d]{0,})([\\+|\\-]?)([\\d]{0,}[.\\]?[\\d]{0,})")
	// UUID 匹配
	//totalExpressR1 := regexp.MustCompile("^([0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})$")
	//numberRb:=numberR.MatchString(caculateValue)
	caculateExpressRb:=caculateExpressR.MatchString(caculateValue)
	//	totalExpressRb:=totalExpressR.MatchString(caculateValue)// 064c92ac-31a7-11e8-9d9b-0242ac110002 true
	//totalExpressRb1:=totalExpressR1.MatchString(caculateValue)
	//	fmt.Printf(" caculateExpressRb=",caculateExpressRb," totalExpressRb=",totalExpressRb," totalExpressRb1=",totalExpressRb1)

	if  caculateExpressRb {
		// 计算表达式 account_subject_left_view.begin_debit_funds.101+account_subject_left_view.begin_debit_funds.102

		fmt.Printf("caculateValue=", caculateValue)
		for {
			if caculateExpressRb {
				arr := caculateExpressR.FindStringSubmatch(caculateValue)
				// account_subject_left_view.end_debit_funds.101
				// "account_subject_left_view"
				// "end_debit_funds"
				// "101"
				caculateValueItem := arr[0]

				fmt.Printf("caculateValueItem=", caculateValueItem)
				// 通过正则匹配查询

				result, errorMessage := calculateForExpress(api, arr, conditionFiledKey, wheres)
				fmt.Printf("errorMessage=", errorMessage)
				caculateValue = strings.Replace(caculateValue, caculateValueItem, result, -1)
				fmt.Printf("caculateValue=", caculateValue)
				caculateExpressRb = caculateExpressR.MatchString(caculateValue)
				if !caculateExpressRb {
					//caculateValue="123.3+2.4-2"
					//expStr := regexp.MustCompile("^([\\d]+\\.?[\\d]+)([\\-|\\+])([\\d]+\\.?[\\d]+)")
					//expStr := regexp.MustCompile("[\\-|\\+]")
					//expArr := expStr.FindStringSubmatch(caculateValue)
					//
					//exp,error :=ExpConvert(expArr)
					//Exp(exp)
					//fmt.Printf("err=",error)
					caculateValue=strings.Replace(caculateValue,"+-","-",-1)
					caculateValue=strings.Replace(caculateValue,"-+","-",-1)
					calResult, error := Calculate(caculateValue)

					if error != nil {
						fmt.Printf("error=", error)
					}
					fmt.Printf("calResult=", calResult)
					return calResult,errorMessage
				}
			} else {
				break
			}
		}

	}else{
		return 0,nil
	}
	var result float64
	resultF,error:=strconv.ParseFloat(caculateValue, 64)
	if error !=nil{
		result=0
	}else{
		result=resultF
	}
	return result,errorMessage
}
// 表达式计算
func calculateForExpress(api adapter.IDatabaseAPI,arr []string,conditionFiledKey string,wheres map[string]WhereOperation)(r string,errorMessage *ErrorMessage){
	// "account_subject_left_view.begin_debit_funds.101"
	// "account_subject_left_view"
	// "begin_debit_funds"
	// "101"
	// 101-PRE-account_period_year
	caculateValueItem:=arr[0]
	caculateFromTable:=arr[1]
	caculateFromFiled:=arr[2]
	caculateConFieldValue:=arr[3]
	var preWhereKey string
	if strings.Contains(caculateConFieldValue,"."){
		arr0:=strings.Split(caculateConFieldValue,".")
		caculateConFieldValue=arr0[0]
		if len(arr0)==2{
			preWhereKey=arr0[1]
		}
		if preWhereKey=="pre"{
			preWhereKey="account_period_year"
		}
		yesYear:=api.ExecFuncForOne("SELECT EXTRACT(YEAR FROM DATE_ADD(NOW(), INTERVAL -1 YEAR)) as preYear;","preYear")
		wheres[preWhereKey]=WhereOperation{
			Operation:"like",
			Value: yesYear+"%",
		}
	}
	fmt.Printf("caculateValueItem",caculateValueItem)
	var optionC QueryOption


	optionC.Table=caculateFromTable
	optionC.Fields=[]string{caculateFromFiled}
	wheres[conditionFiledKey] = WhereOperation{
		Operation: "like",
		Value:     caculateConFieldValue+"%",
	}

	optionC.Wheres=wheres
	var result string
    var resultFloat float64
	dataC, errorMessage := api.Select(optionC)
	for _,value:=range dataC{
		resultIterface:=value[caculateFromFiled]
		if resultIterface!=nil{

			switch resultIterface.(type) {
			case string:
				result=resultIterface.(string)
			case float64:
				resultFloat=resultFloat+resultIterface.(float64)
			}
		}

	}

	if result==""{
		result=strconv.FormatFloat(resultFloat, 'f', -1, 64)
	}
	if result==""{
		result="0"
	}
	return result,nil

}
func responseTableGet(c echo.Context,data interface{},ispaginator bool,filename string,api adapter.IDatabaseAPI,cacheParams string,redisHost string,isNeedCache int,headOption QueryOption) error{
	tableName:=filename
	if c.Request().Header.Get("accept")=="application/octet-stream"||c.QueryParams().Get("accept")=="application/octet-stream" {
		if c.QueryParams().Get("filename")!="" {
			filename =c.QueryParams().Get("filename")
			filename=strings.Replace(strings.ToLower(filename) ,".xlsx","",-1)
		}
		c.Response().Header().Add("Content-Disposition", "attachment; filename="+fmt.Sprintf("%s.xlsx", filename))
		c.Response().Header().Add("Cache-Control", "must-revalidate, post-check=0, pre-check=0")
		c.Response().Header().Add("Pragma", "no-cache")
		xlsx := excelize.NewFile()
		data1:=[]map[string]interface{}{}
		if ispaginator {
			data1=data.(*Paginator).Data.([]map[string]interface{})
		}else {
			data1=data.([]map[string]interface{})
		}

		var templateKey string
		var isDefineMyselfTable bool
		isDefineMyselfTable=tableName=="report_diy_cells_value"
		numAar:= [26]string{1: "A", 2: "B",3:"C",4:"D",5:"E",6:"F",7:"G",8:"H",9:"I",10:"J",11:"K",12:"L",13:"M",14:"N",15:"O",16:"P",17:"Q",18:"R",19:"R",20:"S"}


		// 如果是自定义表
		if isDefineMyselfTable{
			if headOption.Wheres["report_diy_cells_value.report_type"].Value!=nil{
				templateKey= headOption.Wheres["report_diy_cells_value.report_type"].Value.(string)
			}

		}
		var headCols int
		if len(data1)>0{
			//取到表头
			var keys []string
			//keys:=list.New()
			for k, _ := range data1[0] {
				//默认的列
				keys = append(keys, k)
				//keys.PushBack(k)
			}
			//写表头 从模本配置里面获取表头信息 模板key就是tableName
			var headerRows string
			wMapHead := map[string]WhereOperation{}
			wMapHead["template_key"] = WhereOperation{
				Operation: "eq",
				Value:     templateKey,
			}
			optionHead := QueryOption{Wheres: wMapHead, Table: "export_template"}
			data, errorMessage := api.Select(optionHead)
			fmt.Printf("data", data)
			fmt.Printf("errorMessage", errorMessage)
			for _,header:=range data {
				headerRows= header["header_rows"].(string)
				headColsStr:=header["header_cols"].(string)
				headCols,_=strconv.Atoi(headColsStr)
			}
			fmt.Printf("headerRows",headerRows)
			hRows,err:=strconv.Atoi(headerRows)
			if err!=nil{
				fmt.Printf("error",err)
			}
			//fmt.Printf("hRows",hRows)
			//  读取表头内容
			wMapHeadContent := map[string]WhereOperation{}
			wMapHeadContent["template_key"] = WhereOperation{
				Operation: "eq",
				Value:     templateKey,//special_fund_report_detail
			}
			optionHeadContent := QueryOption{Wheres: wMapHead, Table: "export_template_detail"}
			order:=make(map[string]string)
			order["j"]="asc"
			optionHeadContent.Orders=order
			headContent, errorMessage := api.Select(optionHeadContent)
			fmt.Printf("dataContent", headContent)
			fmt.Printf("errorMessage", errorMessage)

			if err!=nil{
				fmt.Printf("error",err)
			}

			if  len(headContent)>0{
				//如果有导出模板信息 覆盖默认的列
				var keys1 []string

				for _,header:=range headContent {
					colName:=header["column_name"].(string)

					keys1 = append(keys1, colName)

				}
				keys=keys1
				for _,header:=range headContent {
					i,err:=strconv.Atoi(header["i"].(string))
					if err!=nil{
						fmt.Printf("err",err)
					}

					j,err1:=strconv.Atoi(header["j"].(string))
					if err1!=nil{
						fmt.Printf("err",err)
					}
					value:=header["value"].(string)
					//if err2!=nil{
					//	fmt.Printf("err",err)
					//}
					xlsx.SetCellValue("Sheet1", excelize.ToAlphaString(j)+strconv.Itoa(i+1), value)
				}
			}
			if !isDefineMyselfTable && len(headContent)<=0{
				for j, k:=range keys{
					xlsx.SetCellValue("Sheet1", excelize.ToAlphaString(j)+strconv.Itoa(1), k)
				}

			}


			// 如果	hRows大于1  说明有合并单元格 并设置其合并内容
			var hdMerge ([]map[string]interface{})
			var reportHeadRs []map[string]interface{}
			var reportHeadItem map[string]interface{}
			if hRows>1{
				hdMapHeadMerge := map[string]WhereOperation{}
				hdMapHeadMerge["template_key"] = WhereOperation{
					Operation: "eq",
					Value:     templateKey,
				}
				optionHdMerge := QueryOption{Wheres: hdMapHeadMerge, Table: "export_header_merge_detail"}
				hdMerge, errorMessage = api.Select(optionHdMerge)
				fmt.Printf("hdMerge", hdMerge)
				fmt.Printf("errorMessage", errorMessage)
				for _,headMergeDeatail:=range hdMerge {
					//i:= headMergeDeatail["i"].(string)
					i,err:=strconv.Atoi(headMergeDeatail["i"].(string))
					fmt.Printf("err=",err)
					if headMergeDeatail["i"].(string)!="LASTE"{
						j,err := strconv.Atoi(headMergeDeatail["j"].(string))
						fmt.Printf("err=",err)
						value:=headMergeDeatail["value"].(string)
						// 有占位符$替换为具体的值
						if strings.Contains(value,"$"){
							reportHead := map[string]WhereOperation{}
							if headOption.Wheres["report_diy_cells_value.farm_id"].Value!=nil{
								reportHead["farm_id"] = WhereOperation{
									Operation: "eq",
									Value:      headOption.Wheres["report_diy_cells_value.farm_id"].Value,
								}
							}
							if headOption.Wheres["report_diy_cells_value.report_type"].Value!=nil{
								reportHead["report_type"] = WhereOperation{
									Operation: "eq",
									Value:      headOption.Wheres["report_diy_cells_value.report_type"].Value,
								}
							}
							if headOption.Wheres["report_diy_cells_value.account_period_year"].Value!=nil{
								reportHead["account_period_year"] = WhereOperation{
									Operation: "eq",
									Value:      headOption.Wheres["report_diy_cells_value.account_period_year"].Value,
								}
							}
							if headOption.Wheres["report_diy_cells_value.account_period_num"].Value!=nil{
								reportHead["account_period_num"] = WhereOperation{
									Operation: "eq",
									Value:      headOption.Wheres["report_diy_cells_value.account_period_num"].Value,
								}
							}
							reportHeadOption := QueryOption{Wheres: reportHead, Table: "report_head"}
							reportHeadRs, errorMessage= api.Select(reportHeadOption)
							fmt.Printf("errorMessage=",errorMessage)
							for _,item:=range reportHeadRs{
								reportHeadItem=item
							}
							value=strings.Replace(value,"$report_head.farm_name",reportHeadItem["farm_name"].(string),-1)
							value=strings.Replace(value,"$report_head.make_time",reportHeadItem["make_time"].(string),-1)
						}
						xlsx.SetCellValue("Sheet1", excelize.ToAlphaString(j)+strconv.Itoa(i+1), value)
					}

				}

			}

			//	xlsx.MergeCell("Sheet1","D2","E3")
			// 合并单元格  从模板里读取合并单元格信息

			wMapHeadMerge := map[string]WhereOperation{}
			wMapHeadMerge["template_key"] = WhereOperation{
				Operation: "eq",
				Value:     templateKey,
			}
			optionHeadMerge := QueryOption{Wheres: wMapHeadMerge, Table: "export_header_merge"}
			headMerge, errorMessage := api.Select(optionHeadMerge)
			fmt.Printf("headMerge", headMerge)
			fmt.Printf("errorMessage", errorMessage)
			for _,headMerge:=range headMerge {
				startItem:= headMerge["start_item"].(string)
				endItem := headMerge["end_item"].(string)
				xlsx.MergeCell("Sheet1",startItem,endItem)
			}

			//写数据A2:ZZ2->An:ZZn
			// 写数据 根据模板里的行标开始写数据
			if hRows!=0{
				for i,d:=range data1{
					if isDefineMyselfTable{
						col,_:=strconv.Atoi(d["col"].(string))
						row,_:=strconv.Atoi(d["row"].(string))
						xlsx.SetCellValue("Sheet1", excelize.ToAlphaString(col)+strconv.Itoa(row+hRows+1), d["value"].(string))
					}else{
						for j, k:=range keys{
							xlsx.SetCellValue("Sheet1", excelize.ToAlphaString(j)+strconv.Itoa(i+hRows+1), d[k])
						}
					}

				}
			}else{
				for i,d:=range data1{
					if isDefineMyselfTable{
						col,_:=strconv.Atoi(d["col"].(string))
						row,_:=strconv.Atoi(d["row"].(string))
						xlsx.SetCellValue("Sheet1", excelize.ToAlphaString(col)+strconv.Itoa(row+hRows+1), d["value"].(string))
					}else{
						for j, k:=range keys{
							xlsx.SetCellValue("Sheet1", excelize.ToAlphaString(j)+strconv.Itoa(i+2), d[k])
						}
					}

				}

			}

			// 写入表位信息
			for _,headMergeDeatail:=range hdMerge {
				//i:= headMergeDeatail["i"].(string)
			//	i,err:=strconv.Atoi(headMergeDeatail["i"].(string))
				fmt.Printf("err=",err)
				data1LenStr:=strconv.Itoa(len(data1))
				data1LenFloat, err:= strconv.ParseFloat(data1LenStr, 64)

				headColsStr:=strconv.Itoa(headCols)
				headColsFloat, err:= strconv.ParseFloat(headColsStr, 64)
				fmt.Printf("err=",err)
				x:=(float64)(data1LenFloat)/(headColsFloat)

				b:=math.Floor(x+0.5)
				cRows:=int(b)
				if headMergeDeatail["i"].(string)=="LASTE"{
					j,err := strconv.Atoi(headMergeDeatail["j"].(string))
					fmt.Printf("err=",err)
					value:=headMergeDeatail["value"].(string)
					// 有占位符$替换为具体的值
					if strings.Contains(value,"$"){
						value=strings.Replace(value,"$report_head.res_person",reportHeadItem["res_person"].(string),-1)
						// submit_person
						value=strings.Replace(value,"$report_head.submit_person",reportHeadItem["submit_person"].(string),-1)
						value=strings.Replace(value,"$report_head.make_time",reportHeadItem["make_time"].(string),-1)
					}
					xlsx.SetCellValue("Sheet1", excelize.ToAlphaString(j)+strconv.Itoa(hRows+1+cRows+1), value)
				}
				startItem:="A"+strconv.Itoa((hRows+1+cRows+1))
				endItem:=numAar[headCols]+strconv.Itoa((hRows+1+cRows+1))
				xlsx.MergeCell("Sheet1",startItem,endItem)
			}

		}

		// Save xlsx file by the given path.
		filePath:= os.TempDir()+string(os.PathSeparator)+uuid.NewV4().String()+".xlsx"
		err := xlsx.SaveAs(filePath)
		if err!=nil {
			return err
		}
		defer os.Remove(filePath)
		fbytes,err := ioutil.ReadFile(filePath)
		if err != nil{
			return err
		}
		return c.Blob(http.StatusOK,"application/octet-stream",fbytes)
	}else{
		var cacheData string
		var err error
		if(isNeedCache==1&&redisHost!=""){
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			defer redisConn.Close()

			cacheData,err=redis.String(redisConn.Do("GET",cacheParams))
			if err!=nil{
				fmt.Printf("err",err)
			}else{
				fmt.Printf("cacheData",cacheData)
			}
		}



		if ispaginator&&cacheData!="QUEUED"&&cacheData!=""&&cacheData!="null"{
			var paginator Paginator
			json.Unmarshal([]byte(cacheData), &paginator)
			return c.JSON( http.StatusOK,paginator)
		}else if cacheData!="QUEUED"&&cacheData!=""&&cacheData!="null"{
			var catcheStruct interface{}
			json.Unmarshal([]byte(cacheData), &catcheStruct)
			return c.JSON( http.StatusOK,catcheStruct)
		}

		//空数据时,输出[] 而不是 null
		if ispaginator && len(data.(*Paginator).Data.([]map[string]interface{}))>0{
			data2:=data.(*Paginator)
			dataByte,err:=json.Marshal(data2)
			if err!=nil{
				fmt.Printf("err",err)
			}
			cacheDataStr:=string(dataByte[:])

			if(isNeedCache==1&&redisHost!=""){
				pool:=newPool(redisHost)
				redisConn:=pool.Get()
				defer redisConn.Close()
				redisConn.Do("SET",cacheParams,cacheDataStr)
				fmt.Printf("cacheDataStr",cacheDataStr)
			}
			return c.JSON( http.StatusOK,data2)
		}else if redisHost!=""&&ispaginator && len(data.(*Paginator).Data.([]map[string]interface{}))==0{
			data2:=data.(*Paginator)
			data2.Data=[]string{}
			return c.JSON( http.StatusOK,data2)
		}else {

			dataByte,err:=json.Marshal(data)
			if err!=nil{
				fmt.Printf("err",err)
			}
			cacheDataStr:=string(dataByte[:])
			//fmt.Printf("cacheDataStr",cacheDataStr)

			if(isNeedCache==1&&redisHost!=""){
				pool:=newPool(redisHost)
				redisConn:=pool.Get()
				defer redisConn.Close()
				redisConn.Do("SET",cacheParams,cacheDataStr)
				fmt.Printf("cacheDataStr",cacheDataStr)
			}

			return c.JSON( http.StatusOK,data)
		}
	}
}

func endpointTableClearCacheSpecific(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		var count int
		//tableName := c.Param("table")
		cacheKey := c.Param("cacheKey")
		cacheKey=cacheKey+"*"
			cacheKeyPattern:=cacheKey
			fmt.Printf("cacheKey=",cacheKey)
			if(redisHost!=""){
				pool:=newPool(redisHost)
				redisConn:=pool.Get()
				defer redisConn.Close()
				val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

				fmt.Println("redis-keys=",val)
				//redisConn.Send("MULTI")
				for i, _ := range val {
					_, err = redisConn.Do("DEL", val[i])
					if err != nil {
						fmt.Println("redis delelte failed:", err)
					}else{
						count=count+1
					}
					fmt.Printf("DEL-CACHE",val[i], err)
				}
			}

			return c.JSON(http.StatusOK, count)

	}
}



func endpointTableGetSpecific(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		tableName := c.Param("table")
		var id string
		id = c.Param("id")

		option ,errorMessage:= parseQueryParams(c)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		option.Table = tableName
		option.Id = id
		rs, errorMessage := api.Select(option)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}
		if(len(rs)==1){
			return c.JSON(http.StatusOK, &rs[0])
		}else if(len(rs)>1){
			errorMessage = &ErrorMessage{ERR_SQL_RESULTS,fmt.Sprintf("Expected one result to be returned by selectOne(), but found: %d", len(rs))}
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}else {
			return echo.NewHTTPError(http.StatusNotFound)
		}
	}
}

func endpointTableCreate(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {

	return func(c echo.Context) error {
		payload, errorMessage := bodyMapOf(c)
		tableName := c.Param("table")
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		// 前置条件处理
		operates,errorMessage:=	mysql.SelectOperaInfo(api,api.GetDatabaseMetadata().DatabaseName+"."+tableName,"POST")
		var operate_condition string
		var operate_content string

		for _,operate:=range operates {
			operate_condition= operate["operate_condition"].(string)
			operate_content = operate["operate_content"].(string)
		}
		var conditionType string
		var conditionFileds string
		var conditionFiledArr [5]string
		var operateCondJsonMap map[string]interface{}
		var operateCondContentJsonMap map[string]interface{}
		fieldList:=list.New()
		// {"conditionType":"JUDGE","conditionTable":"customer.shopping_cart","conditionFields":"[\"customer_id\",\"goods_id\"]"}
		if(operate_condition!=""){
			json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)
			conditionType=operateCondJsonMap["conditionType"].(string)
			conditionFileds=operateCondJsonMap["conditionFields"].(string)
			json.Unmarshal([]byte(conditionFileds), &conditionFiledArr)
		}
		if(operate_content!=""){
			json.Unmarshal([]byte(operate_content), &operateCondContentJsonMap)
		}
		//判断条件类型 如果是JUDGE 判断是否存在 如果存在做操作后动作
		// {"operate_type":"UPDATE","pri_key":"id","action_type":"ACC","action_field":"goods_num"}
		if "JUDGE"==conditionType{
			for _,item:= range conditionFiledArr{
				if item!=""{
					fieldList.PushBack(item)
				}
			}
			//  从配置里获取要判断的字段 并返回对象
			whereOption := map[string]WhereOperation{}
			for e := fieldList.Front(); e != nil; e = e.Next() {
				whereOption[e.Value.(string)] = WhereOperation{
					Operation: "eq",
					Value:     payload[e.Value.(string)].(string),
				}
			}
			querOption := QueryOption{Wheres: whereOption, Table: tableName}
			rsQuery, errorMessage:= api.Select(querOption)
			if errorMessage!=nil{
				fmt.Printf("errorMessage", errorMessage)
			}else{
				fmt.Printf("rs", rsQuery)
			}
			operate_type:=operateCondContentJsonMap["operate_type"].(string)
			pri_key:=operateCondContentJsonMap["pri_key"].(string)
			var pri_key_value string
			action_type:=operateCondContentJsonMap["action_type"].(string)
			action_field:=operateCondContentJsonMap["action_field"].(string)


			action_field_value1:=payload[action_field].(float64)
			fmt.Printf("action_field_value1",action_field_value1)
			action_field_value1_int:=int(action_field_value1)


			var action_field_value int
			// 操作类型是更新 动作类型是累加
			if operate_type=="UPDATE"{
				if action_type=="ACC"{
					for _,rsQ:=range rsQuery {
						pri_key_value=rsQ[pri_key].(string)
						action_field_value0:= rsQ[action_field].(string)
						action_field_value0_int,err0:=strconv.Atoi(action_field_value0)

						if err0!=nil{
							fmt.Printf("err0",err0)
						}
						action_field_value=action_field_value0_int+action_field_value1_int
						break
					}
				}
				actionFiledMap:= map[string]interface{}{}
				actionFiledMap[action_field]=action_field_value
				if pri_key_value!=""{
					rsU,err:=	api.Update(tableName,pri_key_value,actionFiledMap)
					if err!=nil{
						fmt.Print("err=",err)
					}

					rowesAffected,error:=rsU.RowsAffected()
					if error!=nil{
						fmt.Printf("err=",error)
					}
					return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
				}

			}
		}
		primaryColumns:=api.GetDatabaseMetadata().GetTableMeta(tableName).GetPrimaryColumns()
		var priId string
		var priKey string
		for _, col := range primaryColumns {
			if col.Key == "PRI" {
				priKey=col.ColumnName
				if payload[priKey]!=nil{
					priId=payload[priKey].(string)
				}else{
					uuid := uuid.NewV4()
					priId=uuid.String()
					payload[priKey]=priId

				}

				fmt.Printf("priId",priId)
				break;//取第一个主键
			}
		}

		rs, errorMessage := api.Create(tableName, payload)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}
		rowesAffected, err := rs.RowsAffected()
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+err.Error()})
		}
		//添加时清楚缓存
		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"*"
		if strings.Contains(tableName,"related"){
			endIndex:=strings.LastIndex(tableName,"related")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}
		if strings.Contains(tableName,"detail"){
			endIndex:=strings.LastIndex(tableName,"detail")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}
		if(redisHost!=""){
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			defer redisConn.Close()
			val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

			fmt.Println(val, err)
			//redisConn.Send("MULTI")
			for i, _ := range val {
				_, err = redisConn.Do("DEL", val[i])
				if err != nil {
					fmt.Println("redis delelte failed:", err)
				}
				fmt.Printf("DEL-CACHE",val[i], err)
			}
		}

		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointTableColumnDelete(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		//sql:="alter table test1 add  id_test varchar(128) comment 'id_test' comment '测试表';"

		payload, errorMessage := bodyMapOf(c)
		if errorMessage!=nil{
			fmt.Printf("errorMessage=",errorMessage)
			return c.String(http.StatusBadRequest, "error")
		}
		fmt.Printf("errorMessage=",errorMessage)
		tableName := payload["tableName"].(string)
		column := payload["columnName"].(string)

		sql:="alter table "+tableName+" drop column "+column

		errorMessage=api.CreateTableStructure(sql)
		if errorMessage!=nil{
			fmt.Printf("errorMessage=",errorMessage)
		}
		api.UpdateAPIMetadata()
		return c.String(http.StatusOK, "ok")
	}
}

func endpointTableColumnPut(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		//sql:="alter table test1 add  id_test varchar(128) comment 'id_test' comment '测试表';"

		payload, errorMessage := bodyMapOf(c)
		if errorMessage!=nil{
			fmt.Printf("errorMessage=",errorMessage)
			return c.String(http.StatusBadRequest, "error")
		}
		fmt.Printf("errorMessage=",errorMessage)
		tableName := payload["tableName"].(string)
		column := payload["columnName"].(string)
		columnType:=payload["columnType"].(string)
		defaultValue:=payload["defaultValue"]
		columnDes:=payload["columnDes"].(string)
		sql:="alter table "+tableName+" modify column "+column+" "+columnType+" comment '"+columnDes+"';"

		if defaultValue!=""{
			sql="alter table "+tableName+" modify column "+column+" "+columnType+" default '"+defaultValue.(string)+"' comment '"+columnDes+"';"
		}
		errorMessage=api.CreateTableStructure(sql)
		if errorMessage!=nil{
			fmt.Printf("errorMessage=",errorMessage)
			return c.String(http.StatusOK, "ok")
		}
		api.UpdateAPIMetadata()
		return c.String(http.StatusOK, "ok")
	}
}
func endpointFunc(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		// 测试
		rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
	   if error!=nil{
		   return c.String(http.StatusOK, error.ErrorDescription)
	   }
	    fmt.Printf("error",error)
	    fmt.Printf("rs",rs)
	    var result string
	    for _,item:=range rs{
	    	fmt.Printf("")
			result=item["result"].(string)
		}
		return c.String(http.StatusOK, result)
	}
}

func endpointImportData(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		fileHeader,error:=c.FormFile("file")
		fmt.Printf("error=",error)
		templateKey:=c.QueryParam(key.IMPORT_TEMPLATE_KEY)
		where := c.QueryParam(key.KEY_QUERY_WHERE)
		option ,errorMessage:= parseWhereParams(where)
		fmt.Printf("templateKey=",templateKey)
		file,error:=fileHeader.Open()

		//defer file.Close()
		dst, err := os.Create("./upload/" + fileHeader.Filename)
		fmt.Printf("err=",err)
		defer dst.Close()

		//copy the uploaded file to the destination file
		io.Copy(dst, file)
		dst.Close()

		//根据导入模板key查询模板基本信息
		templateWhere := map[string]WhereOperation{}
		templateWhere["template_key"] = WhereOperation{
			Operation: "eq",
			Value:     templateKey,
		}
		templateOption := QueryOption{Wheres: templateWhere, Table: "import_template"}
		data, errorMessage := api.Select(templateOption)
		fmt.Printf("errorMessage", errorMessage)
  		var row_start int
  		var col_start int
		var master_table string
  		var dependency_table string

  		var dependTableKey string
  		var dependTableKeyValue string
  		var orderNum int
		orderNum=1
  		for _,item:=range data{
			row_start_str:=item["row_start"].(string)
			row_start,_=strconv.Atoi(row_start_str)
			col_start_str:=item["col_start"].(string)
			col_start,_=strconv.Atoi(col_start_str)
			master_table=item["table_name"].(string)
			dependency_table=item["dependency_table"].(string)

		}

		primaryColumns:=api.GetDatabaseMetadata().GetTableMeta(dependency_table).GetPrimaryColumns() //  primaryColumns []*ColumnMetadata
		if len(primaryColumns)>0{
			dependTableKey=primaryColumns[0].ColumnName
			dependTableKeyValue=uuid.NewV4().String()
		}

		// 删除已经导入的数据
		var existsDependId string
		if dependency_table!=""{
			option.Table=dependency_table
			data,errorMessage:= api.Select(option)
			fmt.Printf("errorMessage=",errorMessage)
			for _,item:=range data{
					existsDependId=item[dependTableKey].(string)
					api.Delete(dependency_table,existsDependId,nil)
					deMap:=make(map[string]interface{})
					deMap[dependTableKey]=existsDependId
					api.Delete(master_table,nil,deMap)
				}

		}

		templateDetailWhere := map[string]WhereOperation{}
		templateDetailWhere["template_key"] = WhereOperation{
			Operation: "eq",
			Value:     templateKey,
		}


		xlsx,error := excelize.OpenFile("./upload/"+fileHeader.Filename)
		if error!=nil{
			fmt.Printf("error=",error)
			os.Remove("./upload/"+fileHeader.Filename)
			os.Exit(1)
		}
		rows := xlsx.GetRows("Sheet1")
        if rows==nil{
			rows = xlsx.GetRows("汇总表")
		}
		    var rowIndex int
		    if row_start>1{
				tableMap:=make(map[string]interface{})
				for key,value:=range option.Wheres{
					if strings.Contains(key,"."){
						arr:=strings.Split(key,".")
						if len(arr)>0{
							tableMap[arr[1]]=value.Value
						}
					}

				}
				tableMap[dependTableKey]=dependTableKeyValue
				tableMap["create_time"]=time.Now().Format("2006-01-02 15:04:05")
				_,errorMessage:=api.Create(dependency_table,tableMap)
				fmt.Printf("errorMessage=",errorMessage)
				if templateKey=="off_line_report_template"{
					option.Table="report_monitor"
					data,errorMessage:= api.Select(option)
					fmt.Printf("errorMessage=",errorMessage)
					var timeOutDays string
					for _,item:=range data{
						id:=item["id"].(string)
						if item["timeout_days"]!=""{
							timeOutDays=item["timeout_days"].(string)
						}
						api.Delete("report_monitor",id,nil)

					}

					monitorMap:=make(map[string]interface{})
					monitorMap=tableMap
					monitorMap["report_status"]="1"
					if timeOutDays!=""&& timeOutDays!="0"{
						monitorMap["report_status"]="2"
					}
					monitorMap["is_use_account_platform"]="0"
					_,errorMessage=api.Create("report_monitor",monitorMap)
					fmt.Printf("errorMessage=",errorMessage)
				}
			}
	    	rowIndex=0
			for _, row := range rows {
				var tableName string
				rowIndex=rowIndex+1
				if row_start>rowIndex{
					continue
				}
				templateDetailWhere["row_num"] = WhereOperation{
					Operation: "gte",
					Value:     row_start,
				}
				//主表map
				tableMap:=make(map[string]interface{})
				var colIndex int
				for _, colCell := range row {

					// 获取配置数据库表列名和excel列名
					colIndex=colIndex+1

					templateDetailWhere["col_num"] = WhereOperation{
						Operation: "eq",
						Value:     colIndex,
					}
					templateDetailOption := QueryOption{Wheres: templateDetailWhere, Table: "import_template_detail"}
					dataDetail, errorMessage := api.Select(templateDetailOption)
					fmt.Printf("dataDetail", dataDetail)
					fmt.Printf("errorMessage", errorMessage)
					//var colOrder string

					for _,item :=range dataDetail{
						var excelColName string
						if item["table_name"]!=nil{
							tableName=item["table_name"].(string)
						}
						//colOrder=item["column_order"].(string)
						excelColName=item["column_name"].(string)
						if excelColName!="" && colIndex>=col_start{
							b:=api.GetDatabaseTableMetadata(tableName).HaveField(excelColName)
							if b==true{
								tableMap[excelColName]=colCell
							}

						}
					}

				}
				fmt.Println()
				if len(tableMap)>0{
					tableMap["id"]=uuid.NewV4().String()
					tableMap[dependTableKey]=dependTableKeyValue
					tableMap["create_time"]=time.Now().Format("2006-01-02 15:04:05")
					if api.GetDatabaseTableMetadata(tableName).HaveField("order_num"){
						tableMap["order_num"]=orderNum
						orderNum=orderNum+1
					}
					_,errorMessage:=api.Create(tableName,tableMap)
					fmt.Printf("errorMessage=",errorMessage)

				}


			}




		// 清除上传的文件
		os.Remove("./upload/"+fileHeader.Filename)
		return c.String(http.StatusOK, strconv.Itoa(orderNum))
	}
}
func processBlock(line []byte) {
	os.Stdout.Write(line)
}
func ReadAll(filePth string) ([]byte, error) {
	f, err := os.Open(filePth)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(f)
}
func check(e error) {
	if e != nil {
		panic(e)
	}
}
func endpointTableColumnCreate(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		//sql:="alter table test1 add  id_test varchar(128) comment 'id_test' comment '测试表';"

		payload, errorMessage := bodyMapOf(c)
		if errorMessage!=nil{
			fmt.Printf("errorMessage=",errorMessage)
			return c.String(http.StatusBadRequest, "error")
		}
		fmt.Printf("errorMessage=",errorMessage)
		tableName := payload["tableName"].(string)
		column := payload["columnName"].(string)
		afterColumnName := payload["afterColumnName"].(string)
		// isFirst
		isFirst := payload["isFirst"].(string)
		columnType:=payload["columnType"].(string)
		defaultValue:=payload["defaultValue"]
		columnDes:=payload["columnDes"].(string)
		sql:="alter table "+tableName+" add column "+column+" "+columnType+" comment '"+columnDes+"'"

		if defaultValue!=""{
			sql="alter table "+tableName+" add column "+column+" "+columnType+" default '"+defaultValue.(string)+"' comment '"+columnDes+"'"
		}
		if afterColumnName!=""{
			sql=sql+" after "+afterColumnName+";"
		}
		if isFirst=="1"{
			sql=sql+" first;"
		}
		errorMessage=api.CreateTableStructure(sql)
		if errorMessage!=nil{
			fmt.Printf("errorMessage=",errorMessage)
		}
		api.UpdateAPIMetadata()
		return c.String(http.StatusOK, "ok")
	}
}
func endpointTableStructorCreate(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		//sql:="create table test1( id varchar(128) comment 'id',pass varchar(128) comment '密码') comment '测试表';"

		payload, errorMessage := bodyMapOf(c)
		if errorMessage!=nil{
			fmt.Printf("errorMessage=",errorMessage)
			return c.String(http.StatusBadRequest, "error")
		}
		fmt.Printf("errorMessage=",errorMessage)
		tableName := payload["tableName"].(string)
		tableNameDesc := payload["tableNameDesc"].(string)
		tableFields:=payload["tableFields"].(string)
		isReport:=payload["isReport"].(string)
		// ownerOrgId
		ownerOrgId:=payload["ownerOrgId"].(string)
		sql:="create table if not exists "+tableName+"("+tableFields+")comment '"+tableNameDesc+"';"
		tableFields=strings.Replace(tableFields,"PRIMARY KEY(id)","",-1)
		tableFields=strings.Replace(tableFields,"PRIMARY KEY","",-1)
		// primary key
		tableFields=strings.Replace(tableFields,"primary key(id)","",-1)
		tableFields=strings.Replace(tableFields,"primary key","",-1)
		tableNameDesc=strings.Replace(tableNameDesc,"模板","",-1)

		var reportConfig=make(map[string]interface{})
	//	var tcid string
		//tcid=uuid.NewV4().String()
		//reportConfig["template_config_id"]=tcid
		reportConfig["report_name"]=tableName
		reportConfig["report_name_des"]=tableNameDesc
		reportConfig["create_time"]=time.Now().Format("2006-01-02 15:04:05")
		if ownerOrgId!=""{
			// ownerOrgId
			reportConfig["owner_org_id"]=ownerOrgId
		}


		tableNameDesc=tableNameDesc+"详情"
		if strings.Contains(tableName,"_template"){
			tableName=strings.Replace(tableName,"_template","_report",-1)
		}
		detailSql:="create table if not exists "+tableName+"_detail("+tableFields+",id VARCHAR(128)  NOT NULL COMMENT 'id',report_id VARCHAR(128)  NOT NULL COMMENT 'report_id',create_time TIMESTAMP NULL DEFAULT NULL COMMENT '创建时间',update_time TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',PRIMARY KEY (id)"+")comment '"+tableNameDesc+"';"




		if isReport=="1"{
			// 如果是报表 插入报表配置  且创建报表模板表和报表详情表
			_,errorMessage=api.Create("report_template_config",reportConfig)
			fmt.Printf("tableFields=",tableFields)
			fmt.Printf("detailSql=",detailSql)
			errorMessage=api.CreateTableStructure(detailSql)
			if errorMessage!=nil{
				api.Delete("report_template_config",tableName,nil)
				api.CreateTableStructure("drop table if exists "+tableName+"_detail;")
				return c.String(http.StatusInternalServerError, errorMessage.Error())
			}
		}


		errorMessage=api.CreateTableStructure(sql)
		if errorMessage!=nil{
			fmt.Printf("errorMessage",errorMessage)
			api.Delete("report_template_config",tableName,nil)
			api.CreateTableStructure("drop table if exists "+tableName+";")
			return c.String(http.StatusInternalServerError, errorMessage.Error())
		}
		api.UpdateAPIMetadata()
		return c.String(http.StatusOK, "ok")
	}
}


func endpointDeleteMetadataByTable(api adapter.IDatabaseAPI) func(c echo.Context) error {
	return func(c echo.Context) error {
		//sql:="create table test1( id varchar(128) comment 'id',pass varchar(128) comment '密码') comment '测试表';"
		tableName:=c.QueryParam(key.TABLE_NAME)

        sql:="drop table if exists "+tableName+";"
		errorMessage:=api.CreateTableStructure(sql)
		if errorMessage!=nil{
			fmt.Printf("errorMessage=",errorMessage)
			return c.String(http.StatusBadRequest, errorMessage.Error())
		}else{
			var deleteMap=make(map[string]interface{})
			deleteMap["report_name"]=tableName
			api.Delete("report_template_config",nil,deleteMap)
			api.UpdateAPIMetadata()
			return c.String(http.StatusOK, "ok")
		}

	}
}


func endpointTableAsync(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		asyncKey := c.QueryParam(key.ASYNC_KEY)
		fmt.Printf("asyncKey=",asyncKey)
		where := c.QueryParam(key.KEY_QUERY_WHERE)
		option ,errorMessage:= parseWhereParams(where)
		fmt.Printf("option=",option)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}

		 c1 := make (chan int);
		 go asyncCalculete(api,where,asyncKey,c1)

		return c.String(http.StatusOK, "ok")
	}
}


func endpointTableUpdateSpecificField(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		payload, errorMessage := bodyMapOf(c)
		tableName := c.Param("table")

		//fmt.Printf("option=",option)
		where := c.QueryParam("where")
		option ,errorMessage:= parseWhereParams(where)
		fmt.Printf("option=",option)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		rs, errorMessage := api.UpdateBatch(tableName, option.Wheres, payload)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}
		rowesAffected, err := rs.RowsAffected()
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+err.Error()})
		}
		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"*"
		if strings.Contains(tableName,"detail"){
			endIndex:=strings.LastIndex(tableName,"detail")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}
		if(redisHost!=""){
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			defer redisConn.Close()
			val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

			fmt.Println(val, err)
			//redisConn.Send("MULTI")
			if rowesAffected>0{
				for i, _ := range val {
					_, err = redisConn.Do("DEL", val[i])
					if err != nil {
						fmt.Println("redis delelte failed:", err)
					}
					fmt.Printf("DEL-CACHE",val[i], err)
				}
			}

		}

		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}


func endpointTableUpdateSpecific(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		payload, errorMessage := bodyMapOf(c)
		tableName := c.Param("table")
		id := c.Param("id")
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		// 修改之前的信息
		beforeUpdateMap:=make(map[string]interface{})
		var beforeUpdateption QueryOption
		beforeWhere:=make(map[string]WhereOperation)

		beforeWhere["id"]=WhereOperation{
			Operation:"eq",
			Value:id,
		}
		beforeUpdateption.Wheres=beforeWhere
		beforeUpdateption.Table=tableName
		beforeUpdateObj,errorMessage:=api.Select(beforeUpdateption)
		fmt.Printf("errorMessage=",errorMessage)
		if len(beforeUpdateObj)>0{
			beforeUpdateMap=beforeUpdateObj[0]
		}

		rs, errorMessage := api.Update(tableName, id, payload)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}


		rowesAffected, err := rs.RowsAffected()
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+err.Error()})
		}
		if rowesAffected >0{
			var option QueryOption
			var arr []map[string]interface{}
			arr=append(arr,payload)
			option.ExtendedArr=arr
			var firstPrimaryKey string
			masterTableName:=strings.Replace(tableName,"_detail","",-1)
			primaryColumns:=api.GetDatabaseMetadata().GetTableMeta(masterTableName).GetPrimaryColumns() //  primaryColumns []*ColumnMetadata
			if len(primaryColumns)>0{
				firstPrimaryKey=primaryColumns[0].ColumnName
			}
			var option0 QueryOption
			where0:=make(map[string]WhereOperation)
			var masterPrimaryKeyValue string
			where0["id"]=WhereOperation{
				Operation:"eq",
				Value:id,
			}
			option0.Wheres=where0
			option0.Table=tableName
			slaveInfo,errorMessage:=api.Select(option0)
			fmt.Printf("errorMessage=",errorMessage)
			if len(slaveInfo)>0{
				masterPrimaryKeyValue=slaveInfo[0][firstPrimaryKey].(string)
			}

			var option1 QueryOption
			where1:=make(map[string]WhereOperation)

			where1[firstPrimaryKey]=WhereOperation{
				Operation:"eq",
				Value:masterPrimaryKeyValue,
			}
			option1.Wheres=where1
			option1.Table=masterTableName
			masterInfo,errorMessage:=api.Select(option1)

			var extendMap map[string]interface{}
			if len(masterInfo)>0{
				masterPrimaryKeyValue=masterInfo[0][firstPrimaryKey].(string)
				extendMap=masterInfo[0]
			}
			option.ExtendedMap=extendMap
			option.ExtendedMapSecond=beforeUpdateMap

			mysql.PostEvent(api,tableName,"PATCH",nil,option,"")

		}

		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"*"
		if strings.Contains(tableName,"detail"){
			endIndex:=strings.LastIndex(tableName,"detail")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}
		if(redisHost!=""){
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			defer redisConn.Close()
			val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

			fmt.Println(val, err)
			//redisConn.Send("MULTI")
			if rowesAffected>0{
				for i, _ := range val {
					_, err = redisConn.Do("DEL", val[i])
					if err != nil {
						fmt.Println("redis delelte failed:", err)
					}
					fmt.Printf("DEL-CACHE",val[i], err)
				}
			}

		}

		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointTableDelete(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		payload, errorMessage := bodyMapOf(c)
		tableName := c.Param("table")
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		rs, errorMessage := api.Delete(tableName, nil, payload)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}
		rowesAffected, err := rs.RowsAffected()
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+err.Error()})
		}
		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"*"
		if strings.Contains(tableName,"related"){
			endIndex:=strings.LastIndex(tableName,"related")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}

		if(redisHost!=""){
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			defer redisConn.Close()
			val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

			fmt.Println(val, err)
			//redisConn.Send("MULTI")
			for i, _ := range val {
				_, err = redisConn.Do("DEL", val[i])
				if err != nil {
					fmt.Println("redis delelte failed:", err)
				}
				fmt.Printf("DEL-CACHE",val[i], err)
			}
		}

		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointTableDeleteSpecific(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		tableName := c.Param("table")
		id := c.Param("id")
		var option QueryOption
		var ids []string
		ids=append(ids,id)
		option.Ids=ids
		// 前置事件
		mysql.PreEvent(api,tableName,"DELETE",nil,option,"")
		rs, errorMessage := api.Delete(tableName, id, nil)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}
		rowesAffected, err := rs.RowsAffected()
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+err.Error()})
		}
		// 后置事件

		mysql.PostEvent(api,tableName,"DELETE",nil,option,"")
		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"*"
		if strings.Contains(tableName,"related"){
			endIndex:=strings.LastIndex(tableName,"related")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}
		if strings.Contains(tableName,"detail"){
			endIndex:=strings.LastIndex(tableName,"detail")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}
		if(redisHost!=""){
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			defer redisConn.Close()
			val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

			fmt.Println(val, err)
			////redisConn.Send("MULTI")
			if rowesAffected>0{
				for i, _ := range val {
					//err:=_, err = redisConn.Do("DEL", val[i])
					_, err = redisConn.Do("DEL", val[i])
					if err != nil {
						fmt.Println("redis delelte failed:", err)
					}

					fmt.Printf("DEL-CACHE",val[i], err)
				}

			}
		}

		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointBatchCreate(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {
	return func(c echo.Context) error {
		payload, errorMessage := bodySliceOf(c)
		tableName := c.Param("table")
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		var totalRowesAffected int64=0
		r_msg:=[]string{}
		for _, record := range payload {
			_, err := api.Create(tableName, record.(map[string]interface{}))
			if err != nil {
				r_msg=append(r_msg,err.Error())
			} else {
				totalRowesAffected+=1
			}
		}
		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"*"
		if strings.Contains(tableName,"related"){
			endIndex:=strings.LastIndex(tableName,"related")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}

		if(redisHost!=""){
			pool:=newPool(redisHost)
			redisConn:=pool.Get()
			defer redisConn.Close()
			val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

			fmt.Println(val, err)
			//redisConn.Send("MULTI")
			for i, _ := range val {
				_, err = redisConn.Do("DEL", val[i])
				if err != nil {
					fmt.Println("redis delelte failed:", err)
				}
				fmt.Printf("DEL-CACHE",val[i], err)
			}
		}

		return c.JSON(http.StatusOK, &map[string]interface{}{"rowesAffected":totalRowesAffected,"error": r_msg})
	}
}

func endpointServerEndpoints(e *echo.Echo) func(c echo.Context) error {
	return func(c echo.Context) error {
		return c.JSON(http.StatusOK, e.Routes())
	}
}

func bodyMapOf(c echo.Context) (jsonMap map[string]interface{}, errorMessage *ErrorMessage) {
	jsonMap = make(map[string]interface{})
	err := json.NewDecoder(c.Request().Body).Decode(&jsonMap)
	if (err != nil) {
		errorMessage = &ErrorMessage{ERR_PARAMETER, err.Error()}
	}
	return
}



func bodySliceOf(c echo.Context) (jsonSlice []interface{}, errorMessage *ErrorMessage) {
	jsonSlice = make([]interface{}, 0)
	err := json.NewDecoder(c.Request().Body).Decode(&jsonSlice)
	if (err != nil) {
		errorMessage = &ErrorMessage{ERR_PARAMETER, err.Error()}
	}
	return
}

func parseQueryParams(c echo.Context) (option QueryOption, errorMessage *ErrorMessage) {
	option = QueryOption{}
	queryParam := c.QueryParams()
	groupFunc :=c.QueryParam(key.GROUP_FUNC)
    fieldsType:=c.QueryParam(key.KEY_QUERY_FIELDS_TYPE)
    option.FieldsType=fieldsType
	//fmt.Printf("groupFunc",groupFunc)
	option.GroupFunc=groupFunc
	//option.Index, option.Limit, option.Offset, option.Fields, option.Wheres, option.Links, err = parseQueryParams(c)
	option.Limit, _ = strconv.Atoi(c.QueryParam(key.KEY_QUERY_PAGESIZE))  // _limit
	option.Index, _ = strconv.Atoi(c.QueryParam(key.KEY_QUERY_PAGEINDEX)) // _skip
	//排除未传值的情况(==0)
	if option.Limit != 0 {
		if option.Limit <= 0 {
			errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("%s must be >=1", key.KEY_QUERY_PAGESIZE)}
			return
		}
		if (option.Index > 0) {
			option.Offset = (option.Index - 1) * option.Limit
		} else if (option.Index < 0) {
			errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("%s must be >=1", key.KEY_QUERY_PAGEINDEX)}
			return
		}
	} else if option.Index != 0 {
		errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("need to set  %s first,then set %s", key.KEY_QUERY_PAGESIZE, key.KEY_QUERY_PAGEINDEX)}
		return
	}

	option.Fields = make([]string, 0)
	if queryParam[key.KEY_QUERY_FIELDS] != nil { // _fields
		for _, f := range queryParam[key.KEY_QUERY_FIELDS] {
			if (f != "") {
				option.Fields = append(option.Fields, f)
			}
		}
	}
	option.GroupFields = make([]string, 0)
	if queryParam[key.GROUP_BY] != nil { // _fields
		for _, f := range queryParam[key.GROUP_BY] {
			if (f != "") {
				option.GroupFields = append(option.GroupFields, f)
			}
		}
	}

	if queryParam[key.KEY_QUERY_LINK] != nil { // _link
		option.Links = make([]string, 0)
		for _, f := range queryParam[key.KEY_QUERY_LINK] {
			if (f != "") {
				option.Links = append(option.Links, f)
			}
		}
	}
	r := regexp.MustCompile("\\'(.*?)\\'\\.([\\w]+)\\((.*?)\\)")
	if queryParam[key.KEY_QUERY_WHERE] != nil {
		option.Wheres = make(map[string]WhereOperation)
		for _, sWhere := range queryParam[key.KEY_QUERY_WHERE] {
			sWhere = strings.Replace(sWhere, "\"", "'", -1) // replace "
			// 支持同一个参数字符串里包含多个条件
			if strings.Contains(sWhere, "&") {
				subWhereArr := strings.Split(sWhere, "&")
				for _, subWhere := range subWhereArr {
					arr := r.FindStringSubmatch(subWhere)
					if len(arr) == 4 {
						switch arr[2] {
						case "in", "notIn":
							option.Wheres[arr[1]] = WhereOperation{arr[2], strings.Split(arr[3], ",")}
						case "like", "is", "neq", "isNot", "eq":
							option.Wheres[arr[1]] = WhereOperation{arr[2], arr[3]}
						case "lt":
							option.Wheres[arr[1]+".lt"] = WhereOperation{arr[2], arr[3]}
						case  "gt":
							option.Wheres[arr[1]+".gt"] = WhereOperation{arr[2], arr[3]}
						case "lte":
							option.Wheres[arr[1]+".lte"] = WhereOperation{arr[2], arr[3]}
						case  "gte":
							option.Wheres[arr[1]+".gte"] = WhereOperation{arr[2], arr[3]}
						}
					}
				}
			} else {
				arr := r.FindStringSubmatch(sWhere)
				if len(arr) == 4 {
					switch arr[2] {
					case "in", "notIn":
						option.Wheres[arr[1]] = WhereOperation{arr[2], strings.Split(arr[3], ",")}
					case "like", "is", "neq", "isNot", "eq":
						option.Wheres[arr[1]] = WhereOperation{arr[2], arr[3]}

					case "lt":
						option.Wheres[arr[1]+".lt"] = WhereOperation{arr[2], arr[3]}
					case  "gt":
						option.Wheres[arr[1]+".gt"] = WhereOperation{arr[2], arr[3]}
					case "lte":
						option.Wheres[arr[1]+".lte"] = WhereOperation{arr[2], arr[3]}
					case  "gte":
						option.Wheres[arr[1]+".gte"] = WhereOperation{arr[2], arr[3]}
				}

			}
		}

	}
}

	oswr := regexp.MustCompile("\\'(.*?)\\'\\.([\\w]+)\\((.*?)\\)")
	if queryParam[key.KEY_QUERY_OR_WHERE] != nil {
		option.OrWheres = make(map[string]WhereOperation)
		for _, sWhere := range queryParam[key.KEY_QUERY_OR_WHERE] {
			sWhere = strings.Replace(sWhere, "\"", "'", -1) // replace "

					arr := oswr.FindStringSubmatch(sWhere)
					if len(arr) == 4 {
						switch arr[2] {
						case "in", "notIn":
							option.OrWheres[arr[1]] = WhereOperation{arr[2], strings.Split(arr[3], ",")}
						case "like", "is", "neq", "isNot", "eq":
							option.OrWheres[arr[1]+"$"+arr[3]] = WhereOperation{arr[2], arr[3]}
						case "lt":
							option.OrWheres[arr[1]+".lt"] = WhereOperation{arr[2], arr[3]}
						case  "gt":
							option.OrWheres[arr[1]+".gt"] = WhereOperation{arr[2], arr[3]}
						case "lte":
							option.OrWheres[arr[1]+".lte"] = WhereOperation{arr[2], arr[3]}
						case  "gte":
							option.OrWheres[arr[1]+".gte"] = WhereOperation{arr[2], arr[3]}
						}
					}


		}
	}



	orderR := regexp.MustCompile("\\'(.*?)\\'\\((.*?)\\)")
	if queryParam[key.KEY_QUERY_ORDER] != nil {
		option.Orders = make(map[string]string)
		for _, orders := range queryParam[key.KEY_QUERY_ORDER] {
			orders = strings.Replace(orders, "\"", "'", -1) // replace "
			arr := orderR.FindStringSubmatch(orders)
			if len(arr) == 3 {
				option.Orders[arr[1]] = arr[2]

			}
		}
	}

	if queryParam[key.KEY_QUERY_SEARCH] != nil {
		searchStrArray := queryParam[key.KEY_QUERY_SEARCH]
		if searchStrArray[0] != "" {
			option.Search = searchStrArray[0]
		}
	}
	return
}

func parseWhereParams(whereStr string) (option QueryOption, errorMessage *ErrorMessage) {
	option = QueryOption{}

	r := regexp.MustCompile("\\'(.*?)\\'\\.([\\w]+)\\((.*?)\\)")
	if whereStr != "" {
		option.Wheres = make(map[string]WhereOperation)
		    sWhere:=whereStr
			sWhere = strings.Replace(sWhere, "\"", "'", -1) // replace "
			sWhere=strings.Replace(sWhere,"%22","'",-1)
			// 支持同一个参数字符串里包含多个条件
			if strings.Contains(sWhere, "&") {
				subWhereArr := strings.Split(sWhere, "&")
				for _, subWhere := range subWhereArr {
					arr := r.FindStringSubmatch(subWhere)
					if len(arr) == 4 {
						switch arr[2] {
						case "in", "notIn":
							option.Wheres[arr[1]] = WhereOperation{arr[2], strings.Split(arr[3], ",")}
						case "like", "is", "neq", "isNot", "eq":
							option.Wheres[arr[1]] = WhereOperation{arr[2], arr[3]}
						case "lt":
							option.Wheres[arr[1]+".lt"] = WhereOperation{arr[2], arr[3]}
						case  "gt":
							option.Wheres[arr[1]+".gt"] = WhereOperation{arr[2], arr[3]}

						}
					}
				}
			} else {
				arr := r.FindStringSubmatch(sWhere)
				if len(arr) == 4 {
					switch arr[2] {
					case "in", "notIn":
						option.Wheres[arr[1]] = WhereOperation{arr[2], strings.Split(arr[3], ",")}
					case "like", "is", "neq", "isNot", "eq":
						option.Wheres[arr[1]] = WhereOperation{arr[2], arr[3]}

					case "lt":
						option.Wheres[arr[1]+".lt"] = WhereOperation{arr[2], arr[3]}
					case  "gt":
						option.Wheres[arr[1]+".gt"] = WhereOperation{arr[2], arr[3]}

					}

				}

		}
	}

	return
}

func newPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     18,
		MaxActive:   50,
		IdleTimeout: 12*3600 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			//if _, err := c.Do("AUTH", password); err != nil {
			//	c.Close()
			//	return nil, err
			//}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}