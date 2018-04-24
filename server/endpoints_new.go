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
	"github.com/xuri/excelize"
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
		slaveInfoMap,errorMessage:=mysql.JsonArr2map(slaveTableInfo)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errorMessage)
		}
		rowesAffected, errorMessage := api.RelatedCreate(payload)
		// 后置条件处理
		operates, errorMessage := SelectOperaInfo(api, api.GetDatabaseMetadata().DatabaseName+"."+slaveTableName, "POST")
		var operate_condition string
		var operate_content string

		for _, operate := range operates {
			operate_condition = operate["operate_condition"].(string)
			operate_content = operate["operate_content"].(string)
		}
		var conditionType string
		var conditionFileds string
		var conditionFiledArr [5]string
		var operateCondJsonMap map[string]interface{}
		var operateCondContentJsonMap map[string]interface{}
		fieldList := list.New()
		// {"conditionType":"JUDGE","conditionTable":"customer.shopping_cart","conditionFields":"[\"customer_id\",\"goods_id\"]"}
		if (operate_condition != "") {
			json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)
			conditionType = operateCondJsonMap["conditionType"].(string)
			conditionFileds = operateCondJsonMap["conditionFields"].(string)
			json.Unmarshal([]byte(conditionFileds), &conditionFiledArr)
		}
		if (operate_content != "") {
			json.Unmarshal([]byte(operate_content), &operateCondContentJsonMap)
		}
		//判断条件类型 如果是JUDGE 判断是否存在 如果存在做操作后动作  如果是OBTAIN_FROM_LOCAL 从参数里面获取
		// {"operate_type":"UPDATE","pri_key":"id","action_type":"ACC","action_field":"goods_num"}
		if "OBTAIN_FROM_LOCAL" == conditionType {
			for _, item := range conditionFiledArr {
				if item != "" {
					fieldList.PushBack(item)
				}
			}
			//  从参数里获取配置中字段的值
			var count int64
			for e := fieldList.Front(); e != nil; e = e.Next() {

				for _,slave:=range slaveInfoMap{
					if slave[e.Value.(string)]!=nil{
						fielVale := slave[e.Value.(string)].(string)
						operate_type := operateCondContentJsonMap["operate_type"].(string)
						operate_table := operateCondContentJsonMap["operate_table"].(string)

						// 操作类型级联删除
						if operate_type == "CASCADE_DELETE" && fielVale != "" {

							api.Delete(operate_table, fielVale, nil)
							count=count+1

							cacheKeyPattern0:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+operate_table+"*"
							if(redisHost!=""){
								pool:=newPool(redisHost)
								redisConn:=pool.Get()
								defer redisConn.Close()
								val0, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern0))

								fmt.Println(val0, err)
								//redisConn.Send("MULTI")
								for i, _ := range val0 {
									_, err = redisConn.Do("DEL", val0[i])
									if err != nil {
										fmt.Println("redis delelte failed:", err)
									}
									fmt.Printf("DEL-CACHE",val0[i], err)
								}
							}


						}
					}

				}

				rowesAffected=rowesAffected+count
			}
			//	return c.String(http.StatusOK, strconv.FormatInt(rowesAffected, 10))
		}





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
		isRetainMasterInfo:=payload["isRetainMasterInfo"].(string)
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
		data, errorMessage := api.Select(slaveOption)
		fmt.Printf("data", data)
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

		for _,slaveInfo:=range data {
			slaveId:= slaveInfo[slaveColumnName].(string)
			api.Delete(slaveTableName,slaveId,nil)
			count=count+1
		}

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
		rowesAffected, errorMessage := api.RelatedUpdate( payload)
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
				return responseTableGet(c,cacheData,false,tableName,api,params,redisHost,isNeedCache)
			}

			//无需分页,直接返回数组
			data, errorMessage := api.Select(option)
			// 无分页的后置事件
			if isNeedPostEvent==1{
				postEvent(api,tableName,"GET",data,option)
			}
			if errorMessage != nil {
				return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
			}
			return responseTableGet(c,data,false,tableName,api,params,redisHost,isNeedCache)
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
				return responseTableGet(c, &Paginator{int(option.Offset/option.Limit+1),option.Limit, int(math.Ceil(float64(totalCount)/float64(option.Limit))),totalCount,cacheData},true,tableName,api,params,redisHost,isNeedCache)

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
				return responseTableGet(c, &Paginator{int(option.Offset/option.Limit+1),option.Limit, int(math.Ceil(float64(totalCount)/float64(option.Limit))),totalCount,data},true,tableName,api,params,redisHost,isNeedCache)

			}


		}
	}
}
//后置事件处理
func postEvent(api adapter.IDatabaseAPI,tableName string ,equestMethod string,data []map[string]interface{},option QueryOption)(rs []map[string]interface{},errorMessage *ErrorMessage){
	operates,errorMessage:=	SelectOperaInfo(api,api.GetDatabaseMetadata().DatabaseName+"."+tableName,equestMethod)
	fmt.Printf("errorMessage=",errorMessage)
	var operate_condition string
	var operate_content string

	for _,operate:=range operates {
		operate_condition= operate["operate_condition"].(string)
		operate_content = operate["operate_content"].(string)
	}
	var conditionType string
	var conditionTable string
	var conditionFileds string
	var resultFileds string
	var conditionFiledArr [5]string
	var resultFieldsArr [5]string
	var operateCondJsonMap map[string]interface{}
	var operateCondContentJsonMap map[string]interface{}
	fieldList:=list.New()
	// {"conditionType":"JUDGE","conditionTable":"customer.shopping_cart","conditionFields":"[\"customer_id\",\"goods_id\"]"}
	if(operate_condition!=""){
		json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)
		conditionType=operateCondJsonMap["conditionType"].(string)
		conditionFileds=operateCondJsonMap["conditionFields"].(string)
		resultFileds=operateCondJsonMap["resultFields"].(string)
		conditionTable=operateCondJsonMap["conditionTable"].(string)
		json.Unmarshal([]byte(conditionFileds), &conditionFiledArr)
		json.Unmarshal([]byte(resultFileds), &resultFieldsArr)
	}
	if(operate_content!=""){
		json.Unmarshal([]byte(operate_content), &operateCondContentJsonMap)
	}
	for _,item:= range conditionFiledArr{
		if item!=""{
			fieldList.PushBack(item)
		}
	}
	//判断条件类型 如果是JUDGE 判断是否存在 如果存在做操作后动作
	// {"operate_type":"UPDATE","pri_key":"id","action_type":"ACC","action_field":"goods_num"}
	operate_type:=operateCondContentJsonMap["operate_type"].(string)
	// 动态添加列 并为每一列计算出值
	if "DYNAMIC_ADD_COLUMN"==operate_type{

     if "OBTAIN_FROM_SPECIFY"==conditionType{

     	for i,item:=range data{

     		fmt.Printf("i=",i," item=",item," conditionTable=",conditionTable)
     		// 根据主表主键id查询详情
			option.Table=strings.Replace(tableName,"_view","",-1)
			option.Links=[]string{"farm_subject"}
			detailItem, errorMessage:= api.Select(option)
			fmt.Printf("detailItem=",detailItem)

     		// 根据每一行构建查询条件
			whereOption := map[string]WhereOperation{}
			for e := fieldList.Front(); e != nil; e = e.Next() {
				if item[e.Value.(string)]!=nil{
					whereOption[e.Value.(string)] = WhereOperation{
						Operation: "eq",
						Value:     item[e.Value.(string)].(string),
					}
				}

			}
			querOption := QueryOption{Wheres: whereOption, Table: conditionTable}
			rsQuery, errorMessage:= api.Select(querOption)
			if errorMessage!=nil{
				fmt.Printf("errorMessage", errorMessage)
			}else{
				fmt.Printf("rs", rsQuery)
			}
			//


		}


	 }



fmt.Printf("data=",data)


	}
	return data,nil;
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
	var orgId string
	var biz_class string
	var  masterTableName string
	var  slaveTableName string
	option ,errorMessage:= parseWhereParams(where)
	for f,v :=range option.Wheres{
		if strings.Contains(f,".farm_id")&&v.Value!=nil{
			orgId=v.Value.(string)
			masterTableName="report_head"
			slaveTableName=string(f[0:strings.Index(f,".farm_id")])
			break
		}
	}

	// 查询审核过的组织业务类型
	whereOption := map[string]WhereOperation{}
	whereOption["farm_id"] = WhereOperation{
		Operation: "eq",
		Value:     orgId,
	}
	whereOption["is_approval"] = WhereOperation{
		Operation: "eq",
		Value:     1,
	}
	querOption := QueryOption{Wheres: whereOption, Table: "service_farm_list"}
	data, errorMessage:= api.Select(querOption)
	fmt.Printf("data", data)

	for _,item := range data{
		if item["biz_class"]!=nil{
			biz_class=item["biz_class"].(string)
		}

	}
// 构建主表信息并入库
// 1 先判断是否已经存在（根据传入的条件）
// 2 如果已经存在 则删除
// 3 把构建的主表信息插入db

//获取主键信息
	var masterTableColumns []*ColumnMetadata
	var slaveTableColumns []*ColumnMetadata
	var masterIdColumnName string
	var slaveIdColumnName string
	var masterId string
	var slaveId string
	if masterTableName==""{
		return
	}
	masterTableColumns=api.GetDatabaseMetadata().GetTableMeta(masterTableName).Columns

	for _, col := range masterTableColumns {
		if col.Key == "PRI" {
			masterIdColumnName=col.ColumnName
			break;//取第一个主键
		}
	}

	slaveTableColumns=api.GetDatabaseMetadata().GetTableMeta(slaveTableName).Columns
	for _, col := range slaveTableColumns {
		if col.Key == "PRI" {
			slaveIdColumnName=col.ColumnName
			break;//取第一个主键
		}
	}
	option.Table=masterTableName
	masterData, errorMessage:=api.Select(option)
    if errorMessage!=nil{
    	fmt.Printf("errorMessage=",errorMessage)
	}else{
		if masterData!=nil{
			for _,item:=range masterData{
				if item[masterIdColumnName]!=nil{
					masterId=item[masterIdColumnName].(string)
				}
			}
			//如果已经存在删除主表和从表里的信息
			api.Delete(masterTableName,masterId,nil)
			var deleteSlaveMapWhere =make(map[string]interface{})
			deleteSlaveMapWhere[masterIdColumnName]=masterId
			api.Delete(slaveTableName,nil,deleteSlaveMapWhere)
		}

		// 插入主表信息
		var masterMap=make(map[string]interface{})
		masterId=uuid.NewV4().String()
		masterMap[masterIdColumnName]=masterId
		masterMap["create_time"]=time.Now().Format("2006-01-02 15:04:05")
		for _, col := range masterTableColumns {

			for f,w:=range option.Wheres{
				if((masterTableName+"."+col.ColumnName)==f){
					masterMap[col.ColumnName]=strings.Replace(w.Value.(string),"%","",-1)
					break
				}

			}

		}
		api.Create(masterTableName,masterMap)

	}

	fmt.Printf("option=",option,",errorMessage=",errorMessage)
// 根据key查询操作配置
	operates,errorMessage:=	SelectOperaInfoByAsyncKey(api,asyncKey)
	var operate_condition string
	var operateConditionJsonMap map[string]interface{}
	var conditionFieldKey string
	var operate_content string
	var operate_type string
	var operate_table string
	var action_type string
	var conditionFiledArr [5]string
	for _,operate:=range operates {
		operate_content = operate["operate_content"].(string)
		operate_condition = operate["operate_condition"].(string)
	}
	if (operate_condition != "") {
		json.Unmarshal([]byte(operate_condition), &operateConditionJsonMap)
	}

	if operateConditionJsonMap!=nil{
		conditionFieldKey = operateConditionJsonMap["conditionFieldKey"].(string)
		fmt.Printf("conditionFieldKey",conditionFieldKey)
		conditionFileds:=operateConditionJsonMap["conditionFields"].(string)
		json.Unmarshal([]byte(conditionFileds), &conditionFiledArr)
	}

	var operateCondContentJsonMap map[string]interface{}
	if (operate_content != "") {
		json.Unmarshal([]byte(operate_content), &operateCondContentJsonMap)
	}
	if operateCondContentJsonMap!=nil{
		operate_type = operateCondContentJsonMap["operate_type"].(string)
		operate_table = operateCondContentJsonMap["operate_table"].(string)
		action_type = operateCondContentJsonMap["action_type"].(string)
	}


	if operate_type!="" && operate_type=="RELATED_QUERY"{
		// DEPENDY_CACULATE
		if action_type!="" && action_type=="DEPENDY_CACULATE"{
			var optionC QueryOption
			var wheres map[string]WhereOperation
			wheres=make(map[string]WhereOperation)
			var orders map[string]string
			orders=make(map[string]string)
			optionC.Table=operate_table
			if biz_class!=""{
				wheres["biz_class"] = WhereOperation{
					Operation: "eq",
					Value:     biz_class,
				}
			}

			orders["order_num"]="asc"
			optionC.Orders=orders
			optionC.Wheres=wheres
			dataC, errorMessage := api.Select(optionC)
			fmt.Printf("dataC",dataC)
			tableMetadata:=api.GetDatabaseMetadata().GetTableMeta(operate_table)
			var columns []*ColumnMetadata
			if tableMetadata!=nil{
				columns= tableMetadata.Columns
			}
			//查询公共条件
			var wheresExp map[string]WhereOperation
			wheresExp=make(map[string]WhereOperation)
			for _,item:=range conditionFiledArr{
				if item!=""&&option.Wheres[masterTableName+"."+item].Value!=nil {
					wheresExp[item] = WhereOperation{
						Operation:option.Wheres[masterTableName+"."+item].Operation,
						Value:     option.Wheres[masterTableName+"."+item].Value.(string),// 如果是like类型Operation替换掉%
					}
				}

			}
			var lineValueMap map[string]float64
			lineValueMap=make(map[string]float64)
			if errorMessage==nil{
				//计算每一项值 不包括总值
				for _,datac:=range dataC {
					// 主表的关联id
					datac[masterIdColumnName]=masterId
					// 从表主键id
					slaveId=uuid.NewV4().String()
					datac[slaveIdColumnName]=slaveId
					datac["create_time"]=time.Now().Format("2006-01-02 15:04:05")
					for _,column:=range columns{
						var caculateValue string
						if datac[column.ColumnName]!=nil{
							caculateValue=datac[column.ColumnName].(string)
						}
						//caculateValue="11=account_subject_left_view.current_credit_funds.321"
						//caculateValue="1=account_subject_left_view.end_debit_funds.101+account_subject_left_view.end_debit_funds.102"
						//caculateValue="123+account_subject_left_view.begin_debit_funds.102"
						//caculateValue="6=1+2-3-4-5"
						//caculateValue="10=9+8"
						//caculateValue="9=6+7-8"
						//caculateValue="6=1+2-3-4-5"
						//caculateValue="064c92ac-31a7-11e8-9d9b-0242ac110002"
						//r := regexp.MustCompile("\\'(.*?)\\'\\.([\\w]+)\\((.*?)\\)")
						if !strings.Contains(caculateValue,"="){
							continue
						}
						if strings.Contains(column.ColumnName,"des"){
							continue
						}
						arr:=strings.Split(caculateValue,"=")
						var lineNumber string
						if len(arr)>=2{
							lineNumber=arr[0]
							caculateValue=arr[1]
						}

						//numberR := regexp.MustCompile("(^[\\d]+)$")
						caculateExpressR := regexp.MustCompile("([\\w]+)\\.([\\w]+)\\.([\\d]+)")
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

							fmt.Printf("caculateValue=",caculateValue)
							for{
								if caculateExpressRb{
									arr := caculateExpressR.FindStringSubmatch(caculateValue)
									// account_subject_left_view.end_debit_funds.101
									// "account_subject_left_view"
									// "end_debit_funds"
									// "101"
									caculateValueItem:=arr[0]

									fmt.Printf("caculateValueItem=",caculateValueItem)
									// 通过正则匹配查询

									result,errorMessage:=calculateForExpress(api,arr,conditionFieldKey,wheresExp)
									fmt.Printf("errorMessage=",errorMessage)
									caculateValue=strings.Replace(caculateValue,caculateValueItem,result,-1)
									fmt.Printf("caculateValue=",caculateValue)
									caculateExpressRb=caculateExpressR.MatchString(caculateValue)
									if !caculateExpressRb{
										//caculateValue="123.3+2.4-2"
										//expStr := regexp.MustCompile("^([\\d]+\\.?[\\d]+)([\\-|\\+])([\\d]+\\.?[\\d]+)")
										//expStr := regexp.MustCompile("[\\-|\\+]")
										//expArr := expStr.FindStringSubmatch(caculateValue)
										//
										//exp,error :=ExpConvert(expArr)
										//Exp(exp)
										//fmt.Printf("err=",error)

											calResult,error:=Calculate(caculateValue)

											if error!=nil{
												fmt.Printf("error=",error)
											}
											fmt.Printf("calResult=",calResult)



										if  !strings.Contains(column.ColumnName,"des"){
											datac[column.ColumnName]=strconv.FormatFloat(calResult, 'f', -1, 64)
											// 当期
											lineValueMap[lineNumber]=calResult
										}
										//resultStr:=strconv.FormatFloat(calResult, 'f', -1, 64)
										if strings.Contains(column.ColumnName,"begin"){
											lineValueMap[lineNumber+"b"]=calResult
										}else if strings.Contains(column.ColumnName,"end"){
											lineValueMap[lineNumber+"e"]=calResult
										}

									}
								}else{
									break
								}
							}


						}

					}


				}
				//计算每一项的总值
				for _,datac:=range dataC {
					// 主表的关联id
					datac[masterIdColumnName]=masterId
					// 从表主键id
					slaveId=uuid.NewV4().String()
					datac[slaveIdColumnName]=slaveId
					datac["create_time"]=time.Now().Format("2006-01-02 15:04:05")
					for _,column:=range columns{
						var caculateValue string
						if datac[column.ColumnName]!=nil{
								caculateValue=datac[column.ColumnName].(string)
						}
						//caculateValue="11=account_subject_left_view.current_credit_funds.321"
						//caculateValue="1=account_subject_left_view.end_debit_funds.101+account_subject_left_view.end_debit_funds.102"
						//caculateValue="123+account_subject_left_view.begin_debit_funds.102"
						//caculateValue="6=1+2-3-4-5"
						//caculateValue="10=9+8"
						//caculateValue="9=6+7-8"
						//caculateValue="6=1+2-3-4-5"
						//caculateValue="064c92ac-31a7-11e8-9d9b-0242ac110002"
						//r := regexp.MustCompile("\\'(.*?)\\'\\.([\\w]+)\\((.*?)\\)")
						if !strings.Contains(caculateValue,"="){
							continue
						}
						if strings.Contains(column.ColumnName,"des"){
							continue
						}
						arr:=strings.Split(caculateValue,"=")
						var lineNumber string
						if len(arr)>=2{
							lineNumber=arr[0]
							caculateValue=arr[1]
							fmt.Printf("lineNumber=",lineNumber)
						}

						//numberR := regexp.MustCompile("(^[\\d]+)$")

						totalExpressR := regexp.MustCompile("^([\\d]+[.\\]?[\\d]{0,})([\\+|\\-]?)([\\d]{0,}[.\\]?[\\d]{0,})")
						// UUID 匹配
						totalExpressR1 := regexp.MustCompile("^([0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})$")
						//numberRb:=numberR.MatchString(caculateValue)

						totalExpressRb:=totalExpressR.MatchString(caculateValue)// 064c92ac-31a7-11e8-9d9b-0242ac110002 true
						totalExpressRb1:=totalExpressR1.MatchString(caculateValue)
						//fmt.Printf(" caculateExpressRb=",caculateExpressRb," totalExpressRb=",totalExpressRb," totalExpressRb1=",totalExpressRb1)


						if  column.ColumnName!="order_num"&&!strings.Contains(column.ColumnName,"line_number")&&totalExpressRb&&!totalExpressRb1{
							var isFirst=true
							for{
								if column.ColumnName!="order_num"&&totalExpressRb&&!totalExpressRb1{
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
									if strings.Contains(column.ColumnName,"begin"){
										af=lineValueMap[a+"b"]
									}else if strings.Contains(column.ColumnName,"end"){
										af=lineValueMap[a+"e"]
									}else{
										af=lineValueMap[a]
									}
									if !isFirst{
										resultF,error:=strconv.ParseFloat(a, 64)
										if error!=nil{
											fmt.Printf("error=",error)
										}else{
											af=resultF
										}
									}
									if strings.Contains(column.ColumnName,"begin"){
										bf=lineValueMap[b+"b"]
									}else if strings.Contains(column.ColumnName,"end"){
										bf=lineValueMap[b+"e"]
									}else{
										bf=lineValueMap[b]
									}

									calResult:=Calc(operate,af,bf)
									resultStr:=strconv.FormatFloat(calResult, 'f', -1, 64)
									//if itemValue==resultStr
									caculateValue=	strings.Replace(caculateValue,itemValue,resultStr,-1)
									if caculateValue=="0"{
										caculateValue=""
									}
									totalExpressRb=totalExpressR.MatchString(caculateValue)
									totalExpressRb1=totalExpressR1.MatchString(caculateValue)
									isFirst=false
									if itemValue==resultStr||(!totalExpressRb){
										datac[column.ColumnName]=calResult
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
					_, err := api.Create(slaveTableName, datac)
					fmt.Printf("err=",err)
				}
			}


		}
	}

	fmt.Printf("async-test1",time.Now())
	// 向管道传值
	c <- 1
}

// 表达式计算
func calculateForExpress(api adapter.IDatabaseAPI,arr []string,conditionFiledKey string,wheres map[string]WhereOperation)(r string,errorMessage *ErrorMessage){
	// "account_subject_left_view.begin_debit_funds.101"
	// "account_subject_left_view"
	// "begin_debit_funds"
	// "101"
	caculateValueItem:=arr[0]
	caculateFromTable:=arr[1]
	caculateFromFiled:=arr[2]
	caculateConFieldValue:=arr[3]

	fmt.Printf("caculateValueItem",caculateValueItem)
	var optionC QueryOption


	optionC.Table=caculateFromTable
	optionC.Fields=[]string{caculateFromFiled}
	wheres[conditionFiledKey] = WhereOperation{
		Operation: "eq",
		Value:     caculateConFieldValue,
	}

	optionC.Wheres=wheres
	var result string

	dataC, errorMessage := api.Select(optionC)
	for _,value:=range dataC{
		resultIterface:=value[caculateFromFiled]
		if resultIterface!=nil{
			result=resultIterface.(string)
			return result,nil
		}

	}
	return "0",nil

}
func responseTableGet(c echo.Context,data interface{},ispaginator bool,filename string,api adapter.IDatabaseAPI,cacheParams string,redisHost string,isNeedCache int) error{
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
		if len(data1)>0{
			//取到表头
			var keys []string
			for k, _ := range data1[0] {
				keys = append(keys, k)
			}
			//写表头 从模本配置里面获取表头信息 模板key就是tableName
			var headerRows string
			wMapHead := map[string]WhereOperation{}
			wMapHead["template_key"] = WhereOperation{
				Operation: "eq",
				Value:     tableName,
			}
			optionHead := QueryOption{Wheres: wMapHead, Table: "export_template"}
			data, errorMessage := api.Select(optionHead)
			fmt.Printf("data", data)
			fmt.Printf("errorMessage", errorMessage)
			for _,header:=range data {
				headerRows= header["header_rows"].(string)
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
				Value:     tableName,
			}
			optionHeadContent := QueryOption{Wheres: wMapHead, Table: "export_template_detail"}
			headContent, errorMessage := api.Select(optionHeadContent)
			fmt.Printf("dataContent", headContent)
			fmt.Printf("errorMessage", errorMessage)

			if err!=nil{
				fmt.Printf("error",err)
			}

			if  len(headContent)>0{
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
			}else{
				for j, k:=range keys{
					xlsx.SetCellValue("Sheet1", excelize.ToAlphaString(j)+strconv.Itoa(1), k)

				}
			}

			//	xlsx.MergeCell("Sheet1","D2","E3")
			// 合并单元格  从模板里读取合并单元格信息

			wMapHeadMerge := map[string]WhereOperation{}
			wMapHeadContent["template_key"] = WhereOperation{
				Operation: "eq",
				Value:     tableName,
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
					for j, k:=range keys{
						xlsx.SetCellValue("Sheet1", excelize.ToAlphaString(j)+strconv.Itoa(i+hRows+1), d[k])
					}
				}
			}else{
				for i,d:=range data1{
					for j, k:=range keys{
						xlsx.SetCellValue("Sheet1", excelize.ToAlphaString(j)+strconv.Itoa(i+2), d[k])
					}
				}

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
		id := c.Param("id")
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
	rs, errorMessage= api.Select(querOption)
	if errorMessage!=nil{
		fmt.Printf("errorMessage", errorMessage)
	}else{
		fmt.Printf("rs", rs)
	}

	return rs,errorMessage
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

func endpointTableCreate(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {

	return func(c echo.Context) error {
		payload, errorMessage := bodyMapOf(c)
		tableName := c.Param("table")
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		// 前置条件处理
		operates,errorMessage:=	SelectOperaInfo(api,api.GetDatabaseMetadata().DatabaseName+"."+tableName,"POST")
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
		columnType:=payload["columnType"].(string)
		defaultValue:=payload["defaultValue"]
		columnDes:=payload["columnDes"].(string)
		sql:="alter table "+tableName+" add column "+column+" "+columnType+" comment '"+columnDes+"';"

		if defaultValue!=""{
			sql="alter table "+tableName+" add column "+column+" "+columnType+" default '"+defaultValue.(string)+"' comment '"+columnDes+"';"
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
		sql:="create table if not exists "+tableName+"("+tableFields+")comment '"+tableNameDesc+"';"
		tableFields=strings.Replace(tableFields,"PRIMARY KEY (`id`)","",-1)
		tableFields=strings.Replace(tableFields,"PRIMARY KEY","",-1)
		tableNameDesc=strings.Replace(tableNameDesc,"模板","",-1)
		tableNameDesc=tableNameDesc+"详情"
		detailSql:="create table if not exists "+tableName+"_detail("+tableFields+",id VARCHAR(128)  NOT NULL COMMENT 'id',report_id VARCHAR(128)  NOT NULL COMMENT 'report_id',PRIMARY KEY (id)"+")comment '"+tableNameDesc+"';"
		var reportConfig=make(map[string]interface{})
		reportConfig["template_config_id"]=uuid.NewV4().String()
		reportConfig["report_name"]=tableName
		reportConfig["report_name_des"]=tableNameDesc
		reportConfig["create_time"]=time.Now().Format("2006-01-02 15:04:05")



		if isReport=="1"{
			// 如果是报表 插入报表配置  且创建报表模板表和报表详情表
			_,errorMessage=api.Create("report_template_config",reportConfig)
			errorMessage=api.CreateTableStructure(detailSql)

		}


		errorMessage=api.CreateTableStructure(sql)
		if errorMessage!=nil{
			fmt.Printf("errorMessage",errorMessage)
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
		rs, errorMessage := api.Update(tableName, id, payload)
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
		rs, errorMessage := api.Delete(tableName, id, nil)
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
		MaxIdle:     8,
		MaxActive:   10,
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