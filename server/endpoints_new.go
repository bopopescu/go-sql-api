package server

import (
	"github.com/labstack/gommon/log"
	"github.com/shiyongabc/go-sql-api/server/lib"
	"github.com/shiyongabc/go-sql-api/server/util"
	"math/rand"
	"net/http"
	"github.com/shiyongabc/go-sql-api/server/swagger"
	"github.com/labstack/echo"
	"github.com/shiyongabc/go-sql-api/server/static"
	"github.com/shiyongabc/go-sql-api/adapter"
	. "github.com/shiyongabc/go-sql-api/types"

	"math"
	"encoding/json"
	"strconv"
	"fmt"
	"strings"
	"regexp"
	"github.com/shiyongabc/go-sql-api/server/key"
	"github.com/360EntSecGroup-Skylar/excelize"
	"os"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"github.com/garyburd/redigo/redis"

	"github.com/shiyongabc/go-sql-api/adapter/mysql"
	"time"

	"io"
	"bytes"
)


// mountEndpoints to echo server
func mountEndpoints(s *echo.Echo, api adapter.IDatabaseAPI,databaseName string,redisHost string,redisPassword string) {
	s.GET("/api/"+databaseName+"/clear/cache/", endpointTableClearCacheSpecific(api,redisHost,redisPassword)).Name = "clear cache"

	s.POST("/api/"+databaseName+"/related/batch/", endpointRelatedBatch(api,redisHost,redisPassword)).Name = "batch save related table"
	s.DELETE("/api/"+databaseName+"/related/delete/", endpointRelatedDelete(api,redisHost,redisPassword)).Name = "batch delete related table"
	s.PUT("/api/"+databaseName+"/related/record/", endpointRelatedPatch(api,redisHost,redisPassword)).Name = "update related table"
	s.GET("/api/"+databaseName+"/metadata/", endpointMetadata(api)).Name = "Database Metadata"
	s.POST("/api/"+databaseName+"/echo/", endpointEcho).Name = "Echo API"
	s.GET("/api/"+databaseName+"/endpoints/", endpointServerEndpoints(s)).Name = "Server Endpoints"
	s.HEAD("/api/"+databaseName+"/metadata/", endpointUpdateMetadata(api)).Name = "从DB获取最新的元数据"


	s.GET("/api/"+databaseName+"/swagger/", endpointSwaggerJSON(api)).Name = "Swagger Infomation"
	//s.GET("/api/swagger-ui.html", endpointSwaggerUI).Name = "Swagger UI"

	s.GET("/api/"+databaseName+"/:table", endpointTableGet(api,redisHost,redisPassword)).Name = "Retrive Some Records"
	s.POST("/api/"+databaseName+"/:table/query/", endpointTableGet(api,redisHost,redisPassword)).Name = "Retrive Some Records"
	s.POST("/api/"+databaseName+"/:table", endpointTableCreate(api,redisHost,redisPassword)).Name = "Create Single Record"
	s.DELETE("/api/"+databaseName+"/:table", endpointTableDelete(api,redisHost,redisPassword)).Name = "Remove Some Records"

	s.GET("/api/"+databaseName+"/:table/:id", endpointTableGetSpecific(api,redisHost,redisPassword)).Name = "Retrive Record By ID"
	s.DELETE("/api/"+databaseName+"/:table/:id", endpointTableDeleteSpecific(api,redisHost,redisPassword)).Name = "Delete Record By ID"
	s.PATCH("/api/"+databaseName+"/:table/:id", endpointTableUpdateSpecific(api,redisHost,redisPassword)).Name = "Update Record By ID"
	//  根据条件批量修改对象的局部字段
	s.PATCH("/api/"+databaseName+"/:table/where/", endpointTableUpdateSpecificField(api,redisHost,redisPassword)).Name = "PATCH Record By part field"
	s.PUT("/api/"+databaseName+"/:table/where/", endpointTableUpdateSpecificField(api,redisHost,redisPassword)).Name = "Update Record By part field"
	s.PUT("/api/"+databaseName+"/:table/:id", endpointTableUpdateSpecific(api,redisHost,redisPassword)).Name = "Put Record By ID"

	s.POST("/api/"+databaseName+"/:table/batch/", endpointBatchCreate(api,redisHost,redisPassword)).Name = "Batch Create Records"
	s.PUT("/api/"+databaseName+"/:table/batch/", endpointBatchPut(api,redisHost,redisPassword)).Name = "Batch put Records"
    //手动执行异步任务
	s.GET("/api/"+databaseName+"/async/", endpointTableAsync(api,redisHost,redisPassword)).Name = "exec async task"
	//手动执行异步任务1
	s.GET("/api/"+databaseName+"/async/batch", endpointTableAsyncBatch(api,redisHost,redisPassword)).Name = "exec batch async task"

	//手动刷新表结构
	s.GET("/api/"+databaseName+"/table/flush", endpointTableFlush(api,redisHost,redisPassword)).Name = "exec table flush"

	////创建表
	//s.POST("/api/"+databaseName+"/table/", endpointTableStructorCreate(api,redisHost,redisPassword)).Name = "create table structure"
	////查询
	//s.GET("/api/"+databaseName+"/table/", endpointGetMetadataByTable(api)).Name = "query table structure"
	////查询
	//s.DELETE("/api/"+databaseName+"/table/", endpointDeleteMetadataByTable(api)).Name = "delete table structure"
	//
	//
	////添加列
	//s.POST("/api/"+databaseName+"/table/column/", endpointTableColumnCreate(api,redisHost,redisPassword)).Name = "add table column"
	////修改列
	//s.PUT("/api/"+databaseName+"/table/column/", endpointTableColumnPut(api,redisHost,redisPassword)).Name = "put table column"
	////删除列
	//s.DELETE("/api/"+databaseName+"/table/column/", endpointTableColumnDelete(api,redisHost,redisPassword)).Name = "delete table column"

	//导入
	s.POST("/api/"+databaseName+"/import/", endpointImportData(api,redisHost,redisPassword)).Name = "import data to template"
	//执行func
	s.POST("/api/"+databaseName+"/func/", endpointFunc(api,redisHost,redisPassword)).Name = "exec function"

	//手动执行远程api
	s.GET("/api/"+databaseName+"/remote/", endpointRemote(api,redisHost,redisPassword)).Name = "exec remote task"

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
func endpointRelatedBatch(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		tx,error:=api.Connection().Begin()
		lib.Logger.Infof("error=",error)
		payload, errorMessage := bodyMapOf(c)
		if errorMessage!=nil{
			return echo.NewHTTPError(http.StatusBadRequest, errorMessage)
		}
		masterTableName := payload["masterTableName"].(string)
		slaveTableName := payload["slaveTableName"].(string)
		slaveTableInfo:=payload["slaveTableInfo"].(string)
		masterTableInfo:=payload["masterTableInfo"]
		lib.Logger.Info(slaveTableInfo)
		slaveInfoMap,errorMessage:=mysql.JsonArr2map(slaveTableInfo)
		if errorMessage!=nil{
			return echo.NewHTTPError(http.StatusBadRequest, errorMessage)
		}
		masterTableInfoMap:=make(map[string]interface{})
		if payload["masterTableInfo"]!=nil{
			if masterTableInfo.(string)!=""{
				masterTableInfoMap,errorMessage=mysql.Json2map(masterTableInfo.(string))
			}

		}

		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest, errorMessage)
		}
		//operates, errorMessage := mysql.SelectOperaInfo(api, api.GetDatabaseMetadata().DatabaseName+"."+slaveTableName, "POST","0")
		cookie,err := c.Request().Cookie("Authorization")
		if err!=nil{
			lib.Logger.Error("errorMessage=%s",err.Error())
		}
		var jwtToken string
		if cookie!=nil{
			jwtToken=  cookie.Value
		}
		userIdJwtStr:=util.ObtainUserByToken(jwtToken,"userId")

		pool := newPool(redisHost,redisPassword)
		redisConn := pool.Get()
		defer redisConn.Close()
		paramBytes,err:=json.Marshal(payload)
		//lib.Logger.Error("extract paylod err=",err.Error())
		params:=string(paramBytes[:])
		paramV:=util.GetMd5String(params,true,false)
		paramVCache, errC:= redis.String(redisConn.Do("GET", paramV+masterTableName+slaveTableName+"POST"))
		if errC!=nil{
			lib.Logger.Error("errorMessage-redis=",errC.Error())
		}
		lib.Logger.Info("paramV",paramV)
		lib.Logger.Info("paramVC",paramVCache)

		if errC==nil &&paramV==paramVCache{//errC==nil&&len(paramVCache)>0 &&paramV==paramVCache[0]
			errorMessage = &ErrorMessage{ERR_REPEAT_SUBMIT, masterTableName+"-"+slaveTableName+"操作重复提交!"}
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		rowesAffected,masterKey,masterId, errorMessage := api.RelatedCreateWithTx(tx,masterTableName,slaveTableName,payload,userIdJwtStr)
		// 后置条件处理
		if errorMessage != nil {
			tx.Rollback()
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}
		var option QueryOption
		option.ExtendedArr=slaveInfoMap
		masterTableInfoMap[masterKey]=masterId
		option.ExtendedMap=masterTableInfoMap
		//dataR,errorMessage:=mysql.PostEvent(api,tx,slaveTableName,"POST",nil,option,redisHost)
        if errorMessage!=nil{
        	 tx.Rollback()
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}else{
			tx.Commit()
		}

		//if len(dataR)>0{
		//	option.ExtendedMap=dataR[0]
		//}
		// 执行异步任务 c1 := make (chan int);
		c1 := make (chan int);
		go asyncOptionEvent(api,slaveTableName,"POST",option,c1,GenerateRangeNum(3000,4000))
		//请求数据存在缓存中 用于校验重复提交问题
		redisConn.Do("SET", paramV+masterTableName+slaveTableName+"POST",paramV)
		// 设置有效期为1秒
		redisConn.Do("EXPIRE",paramV+masterTableName+slaveTableName+"POST",1)

		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+masterTableName+"*"

		val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

		fmt.Println(val, err)
		//redisConn.Send("MULTI")
		for i, _ := range val {
			_, err = redisConn.Do("DEL", val[i])
			if err != nil {
				fmt.Println("redis delelte failed:", err)
			}
			lib.Logger.Infof("DEL-CACHE",val[i], err)
		}


		cacheKeyPattern1:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+slaveTableName+"*"

		val1, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern1))

		fmt.Println(val1, err)
		//redisConn.Send("MULTI")
		for i, _ := range val1 {
			redisConn.Send("DEL", val1[i])
		}



		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}
func endpointRelatedDelete(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	var count int
	return func(c echo.Context) error {
		tx,error:=api.Connection().Begin()
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

		lib.Logger.Infof("masterTableInfo=",masterTableInfo)
		masterInfoMap:=make(map[string]interface{})
		//slaveInfoMap:=make([]map[string]interface{})

		masterInfoMap,errorMessage=mysql.Json2map(masterTableInfo)

		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		databaseMeta:=api.GetDatabaseMetadata()
		masterMeta:=databaseMeta.GetTableMeta(masterTableName)
		slaveMeta:=databaseMeta.GetTableMeta(slaveTableName)
		var masterIdColumnName string
		var primaryColumns []*ColumnMetadata
		primaryColumns=masterMeta.GetPrimaryColumns()
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
		lib.Logger.Error("errorMessage=%s",errorMessage)

		if isRetainMasterInfo=="0"||isRetainMasterInfo==""{
			_,error=	api.DeleteWithTx(tx,masterTableName,masterId,nil)
			if error!=nil{
				tx.Rollback()
				return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
			}
			count=1;
			if errorMessage!=nil{
				lib.Logger.Error("errorMessage=%s",errorMessage)
			}

		}


		// 删除从表数据  先查出关联的从表记录
		slaveWhere := map[string]WhereOperation{}
		slaveWhere[masterIdColumnName] = WhereOperation{
			Operation: "eq",
			Value:     masterId,
		}
		slaveOption := QueryOption{Wheres: slaveWhere, Table: slaveTableName}
		slaveInfoMap, errorMessage := api.Select(slaveOption)
		lib.Logger.Infof("data", slaveInfoMap)
		lib.Logger.Error("errorMessage=%s", errorMessage)

		var primaryColumnsSlave []*ColumnMetadata
		primaryColumnsSlave=slaveMeta.GetPrimaryColumns()
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
		_,errorMessage=mysql.PreEvent(api,slaveTableName,"DELETE",nil,option,"")
		if errorMessage!=nil{
			// tx.Rollback()
		}
		for _,slaveInfo:=range slaveInfoMap {
			slaveId:= slaveInfo[slaveColumnName].(string)
			_,error=api.DeleteWithTx(tx,slaveTableName,slaveId,nil)
			if error!=nil{
				tx.Rollback()
				return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
			}
			count=count+1
		}

		// 处理后置事件
		var arr []map[string]interface{}
		arr=append(arr,payload)
		option.ExtendedArr=arr

		option.ExtendedMap=masterInfoMap

		// 后置事件
		_,errorMessage=mysql.PostEvent(api,tx,slaveTableName,"DELETE",nil,option,"")

		if errorMessage != nil {
			tx.Rollback()
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage.ErrorDescription)
		}else {
			tx.Commit()
		}
		cacheKeyPattern:="/api"+"/"+databaseMeta.DatabaseName+"/"+masterTableName+"*"
		if(redisHost!=""){
			pool:=newPool(redisHost,redisPassword)
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
				lib.Logger.Infof("DEL-CACHE",val[i], err)
			}
		}


		cacheKeyPattern1:="/api"+"/"+databaseMeta.DatabaseName+"/"+slaveTableName+"*"
		if(redisHost!=""){
			pool:=newPool(redisHost,redisPassword)
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

func endpointRelatedPatch(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		payload, errorMessage := bodyMapOf(c)
		tx,error:=api.Connection().Begin()
		lib.Logger.Error(error)
		masterTableName := payload["masterTableName"].(string)
		slaveTableName := payload["slaveTableName"].(string)
		slaveTableInfo:=payload["slaveTableInfo"].(string)
		masterTableInfo:=payload["masterTableInfo"].(string)
		slaveInfoMap,errorMessage:=mysql.JsonArr2map(slaveTableInfo)
		masterTableInfoMap,errorMessage:=mysql.Json2map(masterTableInfo)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}

		if payload["slaveTableName"]!=nil{
			slaveTableName=payload["slaveTableName"].(string)
		}
		cookie,err := c.Request().Cookie("Authorization")
		if err!=nil{
			lib.Logger.Error("errorMessage=%s",err.Error())
		}
		var jwtToken string
		if cookie!=nil{
			jwtToken=  cookie.Value
		}
		userIdJwtStr:=util.ObtainUserByToken(jwtToken,"userId")


		pool := newPool(redisHost,redisPassword)
		redisConn := pool.Get()
		defer redisConn.Close()
		paramBytes,err:=json.Marshal(payload)
		//lib.Logger.Error("extract paylod err=",err.Error())
		params:=string(paramBytes[:])
		paramV:=util.GetMd5String(params,true,false)
		paramVCache, errC:= redis.String(redisConn.Do("GET", paramV+masterTableName+slaveTableName+"POST"))
		if errC!=nil{
			lib.Logger.Error("obtain fom cache err=",errC.Error())
		}
		lib.Logger.Info("paramV",paramV)
		lib.Logger.Info("paramVC",paramVCache)

		if errC==nil &&paramV==paramVCache{//errC==nil&&len(paramVCache)>0 &&paramV==paramVCache[0]
			errorMessage = &ErrorMessage{ERR_REPEAT_SUBMIT, masterTableName+"-"+slaveTableName+"操作重复提交!"}
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		operates, errorMessage := mysql.SelectOperaInfo(api, api.GetDatabaseMetadata().DatabaseName+"."+slaveTableName, "PATCH","0")
		var option QueryOption
		option.ExtendedArr=slaveInfoMap
		option.ExtendedMap=masterTableInfoMap
		mysql.PreEvent(api,slaveTableName,"PATCH",nil,option,"")

		rowesAffected, errorMessage := api.RelatedUpdateWithTx(tx,operates, payload,userIdJwtStr)

		if errorMessage != nil {
			tx.Rollback()
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage.ErrorDescription)
		}

		//_,errorMessage=mysql.PostEvent(api,tx,slaveTableName,"PATCH",nil,option,"")
		if errorMessage!=nil{
			tx.Rollback()
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage.ErrorDescription)
		}else{
			tx.Commit()
		}
		//请求数据存在缓存中 用于校验重复提交问题
		redisConn.Do("SET", paramV+masterTableName+slaveTableName+"POST",paramV)
		// 设置有效期为1秒
		redisConn.Do("EXPIRE",paramV+masterTableName+slaveTableName+"POST",1)
		c1 := make (chan int);
		go asyncOptionEvent(api,slaveTableName,"PATCH",option,c1,GenerateRangeNum(200,500))
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

func endpointTableGet(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		// cookie,err := c.Request().Cookie("Authorization")
		// fmt.Print("Authorization",cookie.Value)
		tableName := c.Param("table")
		option ,errorMessage:= parseQueryParams(c)
		if errorMessage!=nil{
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		option.Table = tableName
		// 如果是查询商品列表 隔离绿通公司查询商品
		// 如果没有传服务商id  则默认查 绿通公司的商品
		paramBytes,err:=option.MarshalJSON()
		if err!=nil{
			lib.Logger.Infof("err",err)
		}

		orderBytes,err:=json.Marshal(option.Orders)
		if err!=nil{
			lib.Logger.Infof("err",err)
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
		lib.Logger.Infof("params=",params)
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
			lib.Logger.Error("errorMessage=%s", errorMessage)
		}else{
			lib.Logger.Infof("rs", rsQuery)
		}
       // is_need_cache
       var isNeedCache int
		var isNeedPostEvent int
		var appointUser,appointClient string
		var isSubTable int64
       // 返回的字段是否需要计算公式计算
       for _,rsq:=range rsQuery{
		   isNeedCacheStr:=rsq["is_need_cache"].(string)
		   isNeedPostEventStr:=rsq["is_need_post_event"].(string)
		   isNeedCache,err=strconv.Atoi(isNeedCacheStr)
		   isNeedPostEvent,err=strconv.Atoi(isNeedPostEventStr)
           lib.Logger.Print("isNeedPostEvent=",isNeedPostEvent)
		   appointUser=mysql.InterToStr(rsq["appoint_user"])
		   appointClient=mysql.InterToStr(rsq["appoint_client"])
		   isSubTable=mysql.InterToInt(rsq["is_sub_table"])
	   }

		if isNeedCache==1&&redisHost!=""{
			pool:=newPool(redisHost,redisPassword)
			redisConn:=pool.Get()
			cacheData, err = redis.String(redisConn.Do("GET", params))

			if err != nil {
				fmt.Println("redis get failed:", err)
			} else {
				lib.Logger.Infof("Get mykey: %v \n", cacheData)
			}
		}

		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}

		// 指定user或client查询
		cookie,err := c.Request().Cookie("Authorization")
		if err!=nil{
			lib.Logger.Error("errorMessage=%s",err.Error())
		}
		var jwtToken string
		if cookie!=nil{
			jwtToken=  cookie.Value
		}

		if appointUser!=""{
			userId:=util.ObtainUserByToken(jwtToken,"userId")
			if userId==""{
				errorMessage = &ErrorMessage{ERR_AUTH, "认证信息无效!"}
				return echo.NewHTTPError(http.StatusUnauthorized,errorMessage)
			}
			wheres:=make(map[string]WhereOperation)
			if option.Wheres!=nil{
				wheres=option.Wheres
			}
			wheres[appointUser]=WhereOperation{
				Operation:"eq",
				Value:userId,
			}
			option.Wheres=wheres
		}
		if appointClient!=""{
			clientId:=util.ObtainUserByToken(jwtToken,"client_id")
			if clientId==""{
				errorMessage = &ErrorMessage{ERR_AUTH, "认证信息无效!"}
				return echo.NewHTTPError(http.StatusUnauthorized,errorMessage)
			}
			wheres:=make(map[string]WhereOperation)
			if option.Wheres!=nil{
				wheres=option.Wheres
			}
			wheres[appointClient]=WhereOperation{
				Operation:"eq",
				Value:clientId,
			}
			option.Wheres=wheres
		}
		if option.Index==0{
			// 如果缓存中有值 用缓存中的值  否则把查询出来的值放在缓存中
			if cacheData!="QUEUED"&&cacheData!=""&&cacheData!="null"{
				return responseTableGet(c,cacheData,false,tableName,api,params,redisHost,redisPassword,isNeedCache,option)
			}

			//无需分页,直接返回数组
			data, errorMessage := api.Select(option)
			// 多个subTableKey
			subTableKeyArr:=strings.Split(option.SubTableKey,",")
			for _,item:=range subTableKeyArr{
				if item!="" && strings.Contains(item,"."){
					arr:=strings.Split(item,".")
					subTableName:=arr[0]
					subTablePriKey:=arr[1]
					var optionSub QueryOption
					optionSub.Table=subTableName
					subWhere:=make(map[string]WhereOperation)
					for _,item:=range data{
						subWhere[subTablePriKey]=WhereOperation{
							Operation: "eq",
							Value: item[subTablePriKey],

						}
						optionSub.Wheres=subWhere
						subData, errorMessage := api.Select(optionSub)
						if errorMessage!=nil{
							log.Print(errorMessage)
						}
						item[subTableName]=subData
					}
				}
			}


			if errorMessage != nil {
				return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
			}
			return responseTableGet(c,data,false,tableName,api,params,redisHost,redisPassword,isNeedCache,option)
		}else{
			var cacheTotalCount string
			if(isNeedCache==1&&redisHost!=""){
				pool:=newPool(redisHost,redisPassword)
				redisConn:=pool.Get()
				defer redisConn.Close()
				cacheTotalCount,err=redis.String(redisConn.Do("GET",params+"-totalCount"))

			}
			//cacheTotalCount=cacheTotalCount.(string)
			lib.Logger.Infof("cacheTotalCount",cacheTotalCount)
			lib.Logger.Infof("err",err)
			lib.Logger.Infof("cacheData",cacheData)
			if cacheTotalCount!="" &&cacheData!="QUEUED"&&cacheData!=""&&cacheData!="null"&&err==nil{
				totalCount:=0
				totalCount,err:=strconv.Atoi(cacheTotalCount)
				if err!=nil{
					lib.Logger.Infof("err",err)
				}
				return responseTableGet(c, &Paginator{int(option.Offset/option.Limit+1),option.Limit, int(math.Ceil(float64(totalCount)/float64(option.Limit))),totalCount,cacheData},true,tableName,api,params,redisHost,redisPassword,isNeedCache,option)

			}else{

				//分页
				option.IsSubTable=isSubTable
				totalCount,errorMessage:=api.SelectTotalCount(option)
				if errorMessage != nil {
					return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
				}

				data, errorMessage := api.Select(option)
				// 多个subTableKey
				subTableKeyArr:=strings.Split(option.SubTableKey,",")
				for _,item:=range subTableKeyArr{
					if item!="" && strings.Contains(item,"."){
						arr:=strings.Split(item,".")
						subTableName:=arr[0]
						subTablePriKey:=arr[1]
						var optionSub QueryOption
						optionSub.Table=subTableName
						subWhere:=make(map[string]WhereOperation)
						for _,item:=range data{
							subWhere[subTablePriKey]=WhereOperation{
								Operation: "eq",
								Value: item[subTablePriKey],

							}
							optionSub.Wheres=subWhere
							subData, errorMessage := api.Select(optionSub)
							if errorMessage!=nil{
								log.Print(errorMessage)
							}
							item[subTableName]=subData
						}
					}
				}

				if(isNeedCache==1&&redisHost!=""){
					pool:=newPool(redisHost,redisPassword)
					redisConn:=pool.Get()
					defer redisConn.Close()
					redisConn.Do("SET",params+"-totalCount",totalCount)
				}

				if errorMessage != nil {
					return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
				}
				return responseTableGet(c, &Paginator{int(option.Offset/option.Limit+1),option.Limit, int(math.Ceil(float64(totalCount)/float64(option.Limit))),totalCount,data},true,tableName,api,params,redisHost,redisPassword,isNeedCache,option)

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
					lib.Logger.Error("errorMessage=%s",errorMessage)
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
	lib.Logger.Infof("async-test0",time.Now())
	// 模拟异步处理耗费的时间
	time.Sleep(5*time.Second)
	lib.Logger.Infof("async-test1",time.Now())
	lib.Logger.Infof("async-test-result=",(x+y))
	// 向管道传值
	c <- x + y
}
func asyncCalculete(api adapter.IDatabaseAPI,where string,asyncKey string,c chan int){
	lib.Logger.Infof("async-test0",time.Now())
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
	lib.Logger.Infof("option=",option,",errorMessage=",errorMessage)
	if (operate_condition != "") {
		json.Unmarshal([]byte(operate_condition), &operateConditionJsonMap)
	}
	if (operate_content != "") {
		json.Unmarshal([]byte(operate_content), &operateContentJsonMap)
	}
	if operateConditionJsonMap!=nil{
		conditionFieldKey = operateConditionJsonMap["conditionFieldKey"].(string)
		lib.Logger.Infof("conditionFieldKey",conditionFieldKey)
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
			lib.Logger.Infof("dataC",dataC)
			//  如果datac 没有值 查询上期 直到有值为止
		//	period_num, err := strconv.Atoi(option.Wheres["account_period_num"].Value.(string))
		//	lib.Logger.Infof("err=",err)
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
			var isExistsReport int
			     isExistsReport=0
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
					// 判断是否已经存在计算的结果
					if isExistsReport==0{
						var isExistsWhere map[string]WhereOperation
						isExistsWhere=option.Wheres
						isExistsWhere["report_type"]=WhereOperation{
							Operation:"eq",
							Value:operate_report_type,
						}
						var optionExists QueryOption
						optionExists.Wheres=isExistsWhere
						optionExists.Table=report_diy_table_cell_value
						rs, errorMessage:= api.Select(optionExists)
						lib.Logger.Error("errorMessage=%s",errorMessage)
						if len(rs)<=0{
							isExistsReport=1
						}
					}
					if isExistsReport==1{
						dataTempArr=append(dataTempArr,dataTemp)
					}

					if caculateValue!=""{
						if !strings.Contains(caculateValue,"="){
							continue
						}
					// 判断是否包含在更新的科目状态列表中
 					    
						arr:=strings.Split(caculateValue,"=")

						if len(arr)>=2{
							//lineNumber=arr[0]
							caculateValue=arr[1]
						}
						calResult,errorMessage:=calculateByExpressStr(api,conditionFieldKey,wheresExp,caculateValue)
						lib.Logger.Error("errorMessage=%s",errorMessage)




						//dataTempArr=append(dataTempArr,dataTemp)
						caculateExpressR := regexp.MustCompile("([\\w]+)\\.([\\w]+)\\.([\\d]+)")
						caculateExpressRb:=caculateExpressR.MatchString(caculateValue)

						if caculateExpressRb{
							datac["value"]=strconv.FormatFloat(calResult, 'f', 2, 64)
							dataTemp["value"]=calResult
							dataTempArr=append(dataTempArr,dataTemp)
						}
						if cellKey!=""&&caculateExpressRb{
							// 当期
							lineValueMap[cellKey]=calResult
						}

					//	rs,errormessge:=api.Update(report_diy_table_cell,datac["id"],datac)
					//	lib.Logger.Infof("rs=",rs,"errormessge=",errormessge)

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
							//lib.Logger.Infof("lineNumber=",lineNumber)
						}

						//numberR := regexp.MustCompile("(^[\\d]+)$")

						totalExpressR := regexp.MustCompile("^([\\d]+[.\\]?[\\d]{0,})([\\+|\\-]?)([\\d]{0,}[.\\]?[\\d]{0,})")
						// UUID 匹配
						totalExpressR1 := regexp.MustCompile("^([0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12})$")
						//numberRb:=numberR.MatchString(caculateValue)

						totalExpressRb:=totalExpressR.MatchString(caculateValue)// 064c92ac-31a7-11e8-9d9b-0242ac110002 true
						totalExpressRb1:=totalExpressR1.MatchString(caculateValue)
						//lib.Logger.Infof(" caculateExpressRb=",caculateExpressRb," totalExpressRb=",totalExpressRb," totalExpressRb1=",totalExpressRb1)


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
									lib.Logger.Infof("arr=",arr)
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
									lib.Logger.Error("errorMessage=%s",errorMessage)
									aCellKey="cell"+aRowStr+colStr
									af=lineValueMap[aCellKey]
									if !isFirst{
										resultF,error:=strconv.ParseFloat(a, 64)
										if error!=nil{
											lib.Logger.Infof("error=",error)
										}else{
											af=resultF
										}
									}
									var bRowStr string
									if b!=""{
										bRowStr,errorMessage=mysql.ObtainDefineLocal(api,reportType,b)
									}

									lib.Logger.Error("errorMessage=%s",errorMessage)
									bCellKey="cell"+bRowStr+colStr
									bf=lineValueMap[bCellKey]
									calResult:=util.Calc(operate,af,bf)
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
									//	lib.Logger.Infof("shiyongabc")
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
				//option.Table=report_diy_table_cell_value
				//rs, errorMessage:= api.Select(option)
				// 先删除重新计算的数据
				deleteMap:=make(map[string]interface{})
				for k,f:=range option.Wheres{
					deleteMap[k]=f.Value
				}

				//for _,item:=range dataTempArr{
				//	deleteMap["row"]=item["row"]
				//	deleteMap["col"]=item["col"]
				//	_,errorMessage=api.Delete(report_diy_table_cell_value,nil,deleteMap)
				//	lib.Logger.Infof("delete-errorMessage:",errorMessage)
				//}


				for _,item:=range dataTempArr{
					item["id"]=uuid.NewV4().String()
					_, errorMessage:=api.ReplaceCreate(report_diy_table_cell_value,item)
					lib.Logger.Infof("create-error-errorMessage:",errorMessage)
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
			quarter=util.ObtainQuarter(peroidNum)
		}
		existsMonitorWhere["account_quarter"]=WhereOperation{
			Operation:"eq",
			Value:quarter,
		}
		existsMonitorOption.Table="report_monitor"
		existsMonitorOption.Wheres=existsMonitorWhere
		data,errorMessage:= api.Select(existsMonitorOption)
		lib.Logger.Error("errorMessage=%s",errorMessage)
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
		monitorMap["account_quarter"]=quarter
		monitorMap["report_status"]="1"
		if timeOutDays!=""&& timeOutDays!="0"{
			monitorMap["report_status"]="2"
		}
		monitorMap["is_use_account_platform"]="1"
		_,errorMessage=api.Create("report_monitor",monitorMap)
		lib.Logger.Error("errorMessage=%s",errorMessage)
	}
	lib.Logger.Infof("async-test1",time.Now())
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
		lib.Logger.Infof("lineNumber=",lineNumber)
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
	//	lib.Logger.Infof(" caculateExpressRb=",caculateExpressRb," totalExpressRb=",totalExpressRb," totalExpressRb1=",totalExpressRb1)

	if  caculateExpressRb {
		// 计算表达式 account_subject_left_view.begin_debit_funds.101+account_subject_left_view.begin_debit_funds.102

		lib.Logger.Infof("caculateValue=", caculateValue)
		for {
			if caculateExpressRb {
				arr := caculateExpressR.FindStringSubmatch(caculateValue)
				// account_subject_left_view.end_debit_funds.101
				// "account_subject_left_view"
				// "end_debit_funds"
				// "101"
				caculateValueItem := arr[0]

				lib.Logger.Infof("caculateValueItem=", caculateValueItem)
				// 通过正则匹配查询

				result, errorMessage := calculateForExpress(api, arr, conditionFiledKey, wheres)
				lib.Logger.Error("errorMessage=%s", errorMessage)
				caculateValue = strings.Replace(caculateValue, caculateValueItem, result, -1)
				lib.Logger.Infof("caculateValue=", caculateValue)
				caculateExpressRb = caculateExpressR.MatchString(caculateValue)
				if !caculateExpressRb {
					//caculateValue="123.3+2.4-2"
					//expStr := regexp.MustCompile("^([\\d]+\\.?[\\d]+)([\\-|\\+])([\\d]+\\.?[\\d]+)")
					//expStr := regexp.MustCompile("[\\-|\\+]")
					//expArr := expStr.FindStringSubmatch(caculateValue)
					//
					//exp,error :=ExpConvert(expArr)
					//Exp(exp)
					//lib.Logger.Infof("err=",error)
					caculateValue=strings.Replace(caculateValue,"+-","-",-1)
					caculateValue=strings.Replace(caculateValue,"-+","-",-1)
					calResult, error := util.Calculate(caculateValue)

					if error != nil {
						lib.Logger.Infof("error=", error)
					}
					lib.Logger.Infof("calResult=", calResult)
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
		yesYear,errorMessage:=api.ExecFuncForOne("SELECT EXTRACT(YEAR FROM DATE_ADD(NOW(), INTERVAL -1 YEAR)) as preYear;","preYear")
		fmt.Print(errorMessage)
		wheres[preWhereKey]=WhereOperation{
			Operation:"like",
			Value: yesYear+"%",
		}
	}
	lib.Logger.Infof("caculateValueItem",caculateValueItem)
	var optionC QueryOption


	optionC.Table=caculateFromTable
	optionC.Fields=[]string{caculateFromFiled}
	wheres[conditionFiledKey] = WhereOperation{
		Operation: "eq",
		Value:     caculateConFieldValue,
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
func responseTableGet(c echo.Context,data interface{},ispaginator bool,filename string,api adapter.IDatabaseAPI,cacheParams string,redisHost string,redisPassword string,isNeedCache int,headOption QueryOption) error{
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

		templateKey:=tableName
		var isDefineMyselfTable bool
		isDefineMyselfTable=tableName=="report_diy_cells_value"
		numAar:= [26]string{1: "A", 2: "B",3:"C",4:"D",5:"E",6:"F",7:"G",8:"H",9:"I",10:"J",11:"K",12:"L",13:"M",14:"N",15:"O",16:"P",17:"Q",18:"R",19:"R",20:"S"}

        var colsFromStructure int //从结构中获取列数
		// 如果是自定义表
		if isDefineMyselfTable{
			if headOption.Wheres["report_diy_cells_value.report_type"].Value!=nil{
				templateKey= headOption.Wheres["report_diy_cells_value.report_type"].Value.(string)
				// 如果自定义表没值  查询自定义表结构作为导出的内容
				isDefineStructureWhere := map[string]WhereOperation{}
				isDefineStructureWhere["report_type"] = WhereOperation{
					Operation: "eq",
					Value:     templateKey,
				}
				isDefineStructureOption := QueryOption{Wheres: isDefineStructureWhere, Table: "report_diy_cells"}
				isDefineStructureOrder:=make(map[string]string)
				isDefineStructureOrder["row"]="asc"
				isDefineStructureOrder["col"]="asc"
				isDefineStructureOption.Orders=isDefineStructureOrder
				isDefineStructureData, errorMessage := api.Select(isDefineStructureOption)
				colsFromStructureStr:=isDefineStructureData[len(isDefineStructureData)-1]["col"].(string)
				colsFromStructure,_=strconv.Atoi(colsFromStructureStr)
				lib.Logger.Infof("colsFromStructure=",colsFromStructure)
				lib.Logger.Error("errorMessage=%s",errorMessage)
				if len(data1)==0{
					data1=isDefineStructureData
				}
				// 如果没有导出模板 使用默认导出模板

				wMapHead := map[string]WhereOperation{}
				wMapHead["template_key"] = WhereOperation{
					Operation: "eq",
					Value:     templateKey,
				}
				optionHead := QueryOption{Wheres: wMapHead, Table: "export_template"}
				data, errorMessage := api.Select(optionHead)
				lib.Logger.Error("errorMessage=%s",errorMessage)
				if len(data)<=0{
					templateKey="DEFAULT_EXPORT_REPORT_TEMPLATE"
				}
			}

		}
		var headCols int
		var systemEnumMap=make(map[string]interface{})
		if len(data1)>0{
			// 查询枚举值
			enumWhere := map[string]WhereOperation{}
			enumWhere["enum_field"] = WhereOperation{
				Operation: "like",
				Value:     templateKey+"%",
			}
			// 查询枚举值
			enumOption := QueryOption{Wheres: enumWhere, Table: "system_enum"}

			enumData, errorMessage := api.Select(enumOption)
			lib.Logger.Infof("enumData", enumData)
			lib.Logger.Error("errorMessage=%s", errorMessage)
			for _,item:=range enumData{
				if item["enum_field"]!=nil && item["enum_key"]!=nil{
					systemEnumMap[item["enum_field"].(string)+item["enum_key"].(string)]=item["enum_value"]
				}

			}

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
			lib.Logger.Infof("data", data)
			lib.Logger.Error("errorMessage=%s", errorMessage)
			for _,header:=range data {
				headerRows= header["header_rows"].(string)
				headColsStr:=header["header_cols"].(string)
				headCols,_=strconv.Atoi(headColsStr)
			}
			lib.Logger.Infof("headerRows",headerRows)
			hRows,err:=strconv.Atoi(headerRows)
			if err!=nil{
				lib.Logger.Infof("error",err)
			}
			//lib.Logger.Infof("hRows",hRows)
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
			lib.Logger.Infof("dataContent", headContent)
			lib.Logger.Error("errorMessage=%s", errorMessage)

			if err!=nil{
				lib.Logger.Infof("error",err)
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
						lib.Logger.Infof("err",err)
					}

					j,err1:=strconv.Atoi(header["j"].(string))
					if err1!=nil{
						lib.Logger.Infof("err",err)
					}
					value:=header["value"].(string)
					//if err2!=nil{
					//	lib.Logger.Infof("err",err)
					//}

					xlsx.SetCellValue("Sheet1", numAar[j]+strconv.Itoa(i+1), value)
				}
			}
			if !isDefineMyselfTable && len(headContent)<=0{
				for j, k:=range keys{
					xlsx.SetCellValue("Sheet1",  numAar[j]+strconv.Itoa(1), k)
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
				lib.Logger.Infof("hdMerge", hdMerge)
				lib.Logger.Error("errorMessage=%s", errorMessage)
				for _,headMergeDeatail:=range hdMerge {
					//i:= headMergeDeatail["i"].(string)
					i,err:=strconv.Atoi(headMergeDeatail["i"].(string))
					lib.Logger.Infof("err=",err)
					if headMergeDeatail["i"].(string)!="LASTE"{
						j,err := strconv.Atoi(headMergeDeatail["j"].(string))
						lib.Logger.Infof("err=",err)
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
									Operation: "like",
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
							lib.Logger.Error("errorMessage=%s",errorMessage)
							for _,item:=range reportHeadRs{
								reportHeadItem=item
							}
							if reportHeadItem!=nil{
								value=strings.Replace(value,"$report_head.report_title",reportHeadItem["report_title"].(string),-1)
								value=strings.Replace(value,"$report_head.farm_name",reportHeadItem["farm_name"].(string),-1)
								value=strings.Replace(value,"$report_head.make_time",reportHeadItem["make_time"].(string),-1)
								value=strings.Replace(value,"00:00:00","",-1)
							}else{
								value=strings.Replace(value,"$report_head.report_title",reportHeadItem["report_title"].(string),-1)
								value=strings.Replace(value,"$report_head.farm_name","",-1)
								value=strings.Replace(value,"$report_head.make_time","",-1)
							}

						}
						xlsx.SetCellValue("Sheet1", numAar[j]+strconv.Itoa(i+1), value)
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
			lib.Logger.Infof("headMerge", headMerge)
			lib.Logger.Error("errorMessage=%s", errorMessage)
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
						valueObject:=d["value"]
						var valueStr string
						if valueObject!=nil{
							valueStr=valueObject.(string)
						}

						if strings.Contains(valueStr,"="){
							xlsx.SetCellValue("Sheet1", numAar[col]+strconv.Itoa(row+hRows+1), "0")
						}else{
							xlsx.SetCellValue("Sheet1", numAar[col]+strconv.Itoa(row+hRows+1), valueStr)
						}

					}else{
						for j, k:=range keys{
							if d[k]!=nil&&systemEnumMap[templateKey+"."+k+d[k].(string)] !=nil{
								xlsx.SetCellValue("Sheet1", numAar[j]+strconv.Itoa(i+hRows+1), systemEnumMap[templateKey+"."+k+d[k].(string)].(string))
							}else{
								xlsx.SetCellValue("Sheet1", numAar[j]+strconv.Itoa(i+hRows+1), d[k])
							}

						}
					}

				}
			}else{
				for i,d:=range data1{
					if isDefineMyselfTable{
						col,_:=strconv.Atoi(d["col"].(string))
						row,_:=strconv.Atoi(d["row"].(string))
						xlsx.SetCellValue("Sheet1", numAar[col]+strconv.Itoa(row+hRows+1), d["value"].(string))
					}else{
						for j, k:=range keys{
							xlsx.SetCellValue("Sheet1", numAar[j]+strconv.Itoa(i+2), d[k])
						}
					}

				}

			}

			// 写入表位信息
			for _,headMergeDeatail:=range hdMerge {
				//i:= headMergeDeatail["i"].(string)
			//	i,err:=strconv.Atoi(headMergeDeatail["i"].(string))
				lib.Logger.Infof("err=",err)
				data1LenStr:=strconv.Itoa(len(data1))
				data1LenFloat, err:= strconv.ParseFloat(data1LenStr, 64)

				//headColsStr:=strconv.Itoa(headCols)
				headColsFloat, err:= strconv.ParseFloat(strconv.Itoa(colsFromStructure+1), 64)
				lib.Logger.Infof("err=",err)
				x:=(float64)(data1LenFloat)/(headColsFloat)

				b:=math.Floor(x+0.5)
				cRows:=int(b)
				if headMergeDeatail["i"].(string)=="LASTE"{
					j,err := strconv.Atoi(headMergeDeatail["j"].(string))
					lib.Logger.Infof("err=",err)
					value:=headMergeDeatail["value"].(string)
					// 有占位符$替换为具体的值
					if strings.Contains(value,"$"){

						if reportHeadItem!=nil{
							value=strings.Replace(value,"$report_head.res_person",reportHeadItem["res_person"].(string),-1)
							// submit_person
							value=strings.Replace(value,"$report_head.submit_person",reportHeadItem["submit_person"].(string),-1)
							value=strings.Replace(value,"$report_head.make_time",reportHeadItem["make_time"].(string),-1)
							// 00:00:00
							value=strings.Replace(value,"00:00:00","",-1)
						}else{
							value=strings.Replace(value,"$report_head.res_person","",-1)
							// submit_person
							value=strings.Replace(value,"$report_head.submit_person","",-1)
							value=strings.Replace(value,"$report_head.make_time","",-1)
							// 00:00:00
							value=strings.Replace(value,"00:00:00","",-1)
						}


					}
					xlsx.SetCellValue("Sheet1", numAar[j]+strconv.Itoa(hRows+1+cRows+1), value)
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
			pool:=newPool(redisHost,redisPassword)
			redisConn:=pool.Get()
			defer redisConn.Close()

			cacheData,err=redis.String(redisConn.Do("GET",cacheParams))
			if err!=nil{
				lib.Logger.Infof("err",err)
			}else{
				lib.Logger.Infof("cacheData",cacheData)
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
				lib.Logger.Infof("err",err)
			}
			cacheDataStr:=string(dataByte[:])

			if(isNeedCache==1&&redisHost!=""){
				pool:=newPool(redisHost,redisPassword)
				redisConn:=pool.Get()
				defer redisConn.Close()
				redisConn.Do("SET",cacheParams,cacheDataStr)
				lib.Logger.Infof("cacheDataStr",cacheDataStr)
			}
			return c.JSON( http.StatusOK,data2)
		}else if redisHost!=""&&ispaginator && len(data.(*Paginator).Data.([]map[string]interface{}))==0{
			data2:=data.(*Paginator)
			data2.Data=[]string{}
			return c.JSON( http.StatusOK,data2)
		}else {

			dataByte,err:=json.Marshal(data)
			if err!=nil{
				lib.Logger.Infof("err",err)
			}
			cacheDataStr:=string(dataByte[:])
			//lib.Logger.Infof("cacheDataStr",cacheDataStr)

			if(isNeedCache==1&&redisHost!=""){
				pool:=newPool(redisHost,redisPassword)
				redisConn:=pool.Get()
				defer redisConn.Close()
				redisConn.Do("SET",cacheParams,cacheDataStr)
				lib.Logger.Infof("cacheDataStr",cacheDataStr)
			}

			return c.JSON( http.StatusOK,data)
		}
	}
}

func endpointTableClearCacheSpecific(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		var count int
		//tableName := c.Param("table")
		cacheKey := c.Param("cacheKey")
		cacheKey=cacheKey+"*"
			cacheKeyPattern:=cacheKey
			lib.Logger.Infof("cacheKey=",cacheKey)
			if(redisHost!=""){
				pool:=newPool(redisHost,redisPassword)
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
					lib.Logger.Infof("DEL-CACHE",val[i], err)
				}
			}

			return c.JSON(http.StatusOK, count)

	}
}



func endpointTableGetSpecific(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		tableName := c.Param("table")
		var id string
		id = c.Param("id")
		option ,errorMessage:= parseQueryParams(c)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		isInject:=util.ValidSqlInject(id)
		if isInject{
			errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("bad param")}
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		whereOption := map[string]WhereOperation{}
		whereOption["view_name"] = WhereOperation{
			Operation: "eq",
			Value:     tableName,
		}
		viewQuerOption := QueryOption{Wheres: whereOption, Table: "view_config"}
		rsQuery, errorMessage:= api.Select(viewQuerOption)
		if errorMessage!=nil{
			lib.Logger.Error("errorMessage=%s", errorMessage)
		}else{
			lib.Logger.Infof("rs", rsQuery)
		}
		// is_need_cache
		var isSubTable int64
		// 返回的字段是否需要计算公式计算
		for _,rsq:=range rsQuery{
			isSubTable=mysql.InterToInt(rsq["is_sub_table"])
		}

		option.IsSubTable=isSubTable
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

func endpointTableCreate(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {

	return func(c echo.Context) error {
		tx,error:=api.Connection().Begin()
		lib.Logger.Error(error)
		payload, errorMessage := bodyMapOf(c)
		tableName := c.Param("table")
		meta:=api.GetDatabaseMetadata().GetTableMeta(tableName)
		if meta.HaveField("create_time"){
			payload["create_time"]=time.Now().Format("2006-01-02 15:04:05")
		}
		cookie,err := c.Request().Cookie("Authorization")
		if err!=nil{
			lib.Logger.Error("errorMessage=%s",err.Error())
		}
		var jwtToken string
		if cookie!=nil{
			jwtToken=  cookie.Value
		}
		userIdJwtStr:=util.ObtainUserByToken(jwtToken,"userId")

		lib.Logger.Infof("userIdJwtStr=",userIdJwtStr)
		if meta.HaveField("submit_person"){
			payload["submit_person"]=userIdJwtStr
		}

		pool := newPool(redisHost,redisPassword)
		redisConn := pool.Get()
		defer redisConn.Close()
		paramBytes,err:=json.Marshal(payload)
		//lib.Logger.Error("extract paylod err=",err.Error())
		params:=string(paramBytes[:])
		paramV:=util.GetMd5String(params,true,false)
		paramVCache, errC:= redis.String(redisConn.Do("GET", paramV+tableName+"POST"))
		if errC!=nil{
			lib.Logger.Error("obtain fom cache err=",errC.Error())
		}
        lib.Logger.Info("paramV",paramV)
		lib.Logger.Info("paramVC",paramVCache)

		if errC==nil &&paramV==paramVCache{//errC==nil&&len(paramVCache)>0 &&paramV==paramVCache[0]
			errorMessage = &ErrorMessage{ERR_REPEAT_SUBMIT, tableName+"操作重复提交!"}
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}



		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
        // 前置事件
		var option QueryOption


		primaryColumns:=meta.GetPrimaryColumns()
		var priId interface{}
		var priKey string
		for _, col := range primaryColumns {
			if col.Key == "PRI" {
				priKey=col.ColumnName
				priDataType:=col.DataType
				if payload[priKey]!=nil{
					priId=payload[priKey]
				}else{
					// 如果是int或bigint 创建分布式整型id
					if priDataType=="bigint" || priDataType=="int"{
						uuid := util.GetSnowflakeId()
						priId=uuid
						payload[priKey]=priId
					}else{
						uuid := uuid.NewV4()
						priId=uuid
						payload[priKey]=uuid.String()
					}


				}

				lib.Logger.Infof("priId",priId)
				break;//取第一个主键
			}
		}
		option.ExtendedMap=payload
		option.PriKey=priKey

		if cookie==nil{
			option.Authorization=""
		}else{
			option.Authorization=cookie.Value
		}


		if err!=nil{
			lib.Logger.Infof("err=",err.Error())
		}


		lib.Logger.Infof("userIdJwtStr=",userIdJwtStr)
        if meta.HaveField("submit_person"){
			option.ExtendedMap["submit_person"]=userIdJwtStr
		}
		data,errorMessage:=mysql.PreEvent(api,tableName,"POST",nil,option,redisHost)
		if len(data)>0{
			option.ExtendedMap=data[0]
		}
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}
		rs, error := api.CreateWithTx(tx,tableName, option.ExtendedMap)
		//rs, errorMessage := api.Create(tableName, option.ExtendedMap)
		//fmt.Print("sql",sql)
		//rs,error:=tx.Exec(sql)

		// 后置事件的事物回滚 需要用tx来提交执行
		//tx.Commit()
		if error != nil {
			tx.Rollback()
			return echo.NewHTTPError(http.StatusInternalServerError,error.Error())
		}
		rowesAffected, err := rs.RowsAffected()
		// 后置事件
		_,errorMessage=mysql.PostEvent(api,tx,tableName,"POST",nil,option,redisHost)
       if errorMessage!=nil{
	      tx.Rollback() // 回滚
		   return echo.NewHTTPError(http.StatusInternalServerError,ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+errorMessage.Error()})
	   }else{
       	  tx.Commit()
	   }
		//请求数据存在缓存中 用于校验重复提交问题
		redisConn.Do("SET", paramV+tableName+"POST",paramV)
		// 设置有效期为1秒
		redisConn.Do("EXPIRE",paramV+tableName+"POST",1)
		// 执行异步任务 c1 := make (chan int);
		c1 := make (chan int);
		go asyncOptionEvent(api,tableName,"POST",option,c1,GenerateRangeNum(500,800))
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

		val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))
		fmt.Println(val, err)
		//redisConn.Send("MULTI")
		for i, _ := range val {
			_, err = redisConn.Do("DEL", val[i])
			if err != nil {
				fmt.Println("redis delelte failed:", err)
			}
			lib.Logger.Infof("DEL-CACHE",val[i], err)
		}

      println("rowesAffected=",rowesAffected,"pri",priId)
       if rowesAffected>0 {
		   return c.String(http.StatusOK, mysql.InterToStr(priId))
	   }else{
		   return c.String(http.StatusInternalServerError, errorMessage.ErrorDescription)
	   }

	}
}

func endpointTableColumnDelete(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		//sql:="alter table test1 add  id_test varchar(128) comment 'id_test' comment '测试表';"

		payload, errorMessage := bodyMapOf(c)
		if errorMessage!=nil{
			lib.Logger.Error("errorMessage=%s",errorMessage)
			return c.String(http.StatusBadRequest, "error")
		}
		lib.Logger.Error("errorMessage=%s",errorMessage)
		tableName := payload["tableName"].(string)
		column := payload["columnName"].(string)

		sql:="alter table "+tableName+" drop column "+column

		errorMessage=api.CreateTableStructure(sql)
		if errorMessage!=nil{
			lib.Logger.Error("errorMessage=%s",errorMessage)
		}
		api.UpdateAPIMetadata()
		return c.String(http.StatusOK, "ok")
	}
}

func endpointTableColumnPut(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		//sql:="alter table test1 add  id_test varchar(128) comment 'id_test' comment '测试表';"

		payload, errorMessage := bodyMapOf(c)
		if errorMessage!=nil{
			lib.Logger.Error("errorMessage=%s",errorMessage)
			return c.String(http.StatusBadRequest, "error")
		}
		lib.Logger.Error("errorMessage=%s",errorMessage)
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
			lib.Logger.Error("errorMessage=%s",errorMessage)
			return c.String(http.StatusOK, "ok")
		}
		api.UpdateAPIMetadata()
		return c.String(http.StatusOK, "ok")
	}
}

func endpointRemote(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		// 测试
		authorization:=c.QueryParam(key.AUTHORIZATION_KEY)
		client := &http.Client{}
		//生成要访问的url
		url := "http://bigdata.vimi8.top/industrial/product/0006ebc0-dce8-4291-87e0-76dbf69bbe41"
        fmt.Print("url=",url)
		//提交请求
		reqest, err := http.NewRequest("GET", url, nil)

		//增加header选项
		reqest.Header.Set("Cookie", "Authorization=bearer%20"+authorization)
		reqest.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36")
		reqest.Header.Set("Accept", "application/json, text/plain, */*")
		reqest.Header.Set("Content-type", "application/json")
		if err != nil {
			panic(err)
		}
		//处理返回结果
		response, _ := client.Do(reqest)
		fmt.Print("response", response)
		if response.StatusCode == 200 {
			body, _ := ioutil.ReadAll(response.Body)
			fmt.Println(string(body))
		}
		// POST
		//req := `{"name":"junneyang", "age": 88}`
		//req_new := bytes.NewBuffer([]byte(req))
		//request, _ = http.NewRequest("POST", "http://10.67.2.252:8080/test/", req_new)
		//request.Header.Set("Content-type", "application/json")
		//response, _ = client.Do(request)
		//if response.StatusCode == 200 {
		//	body, _ := ioutil.ReadAll(response.Body)
		//	fmt.Println(string(body))
		//}
		return c.String(http.StatusOK, strconv.Itoa(response.StatusCode))
	}
}
func endpointFunc(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		// 测试
	//	rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")

		rs,error:= api.ExecFunc("SELECT stu_no,project_name into stuNo,projectName from test.stu_score limit 1;")

	   if error!=nil{
		   return c.String(http.StatusOK, error.ErrorDescription)
	   }
	    lib.Logger.Infof("error",error)
	    lib.Logger.Infof("rs",rs)
	    var result string
		var a []string
	    for _,item:=range rs{
	    	lib.Logger.Infof("")
			result=strings.Join(a,item["stuNo"].(string))
			result=strings.Join(a,item["projectName"].(string))
		}
		return c.String(http.StatusOK, result)
	}
}

func endpointImportData(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		tx,error:=api.Connection().Begin()
		fileHeader,error:=c.FormFile("file")
		lib.Logger.Infof("error=",error)
		templateKey:=c.QueryParam(key.IMPORT_TEMPLATE_KEY)

		queryParam := c.QueryParams()
		wheres:=queryParam[key.KEY_QUERY_WHERE]
		option ,errorMessage:= parseWheres(wheres)
		lib.Logger.Infof("templateKey=",templateKey)
		file,error:=fileHeader.Open()

		//defer file.Close()
		dst, err := os.Create("./upload/" + fileHeader.Filename)
		lib.Logger.Infof("err=",err)
		defer dst.Close()

		cookie,err := c.Request().Cookie("Authorization")
		if err!=nil{
			lib.Logger.Error("errorMessage=%s",err.Error())
		}
		var jwtToken string
		if cookie!=nil{
			jwtToken=  cookie.Value
		}
		userIdJwtStr:=util.ObtainUserByToken(jwtToken,"userId")
		submitPerson:=userIdJwtStr


		//copy the uploaded file to the destination file
		io.Copy(dst, file)
		dst.Close()
        importBatchNo:= uuid.NewV4().String()

		//根据导入模板key查询模板基本信息
		templateWhere := map[string]WhereOperation{}
		templateWhere["template_key"] = WhereOperation{
			Operation: "eq",
			Value:     templateKey,
		}
		templateOption := QueryOption{Wheres: templateWhere, Table: "import_template"}
		data, errorMessage := api.Select(templateOption)
		lib.Logger.Error("errorMessage=%s", errorMessage)
  		var row_start int
  		var col_start int
		var col_end int
		var master_table string
  		//var dependency_table string

		var tableKey string
		var tableKeyValue string

  		//var dependTableKey string
  		//var dependTableKeyValue string
  		var extractParam string
  		var extractParamMap map[string]interface{}
		var extractParamArr [5]string
		var importBuffer bytes.Buffer
		var systemEnumMap =make(map[string]interface{})
		var tableDataTypeMap =make(map[string]interface{})
        totalCount:=1
		//var orderNum int
		//orderNum=1
  		for _,item:=range data{
			row_start_str:=item["row_start"].(string)
			row_start,_=strconv.Atoi(row_start_str)
			col_start_str:=item["col_start"].(string)
			col_start,_=strconv.Atoi(col_start_str)

			col_end_str:=item["col_end"].(string)
			col_end,_=strconv.Atoi(col_end_str)
			master_table=item["table_name"].(string)
			//if item["dependency_table"]!=nil{
			//	dependency_table=item["dependency_table"].(string)
			//}
			if item["extract_param"]!=nil{
				extractParam=item["extract_param"].(string)
				json.Unmarshal([]byte(extractParam), &extractParamMap)

			}

		}

		// 查询枚举值
		enumWhere := map[string]WhereOperation{}
		enumWhere["enum_field"] = WhereOperation{
			Operation: "like",
			Value:     master_table+"%",
		}
		// 查询枚举值
		enumOption := QueryOption{Wheres: enumWhere, Table: "system_enum"}

		enumData, errorMessage := api.Select(enumOption)
		lib.Logger.Infof("enumData", enumData)
		lib.Logger.Error("errorMessage=%s", errorMessage)
		for _,item:=range enumData{
			if item["enum_field"]!=nil && item["enum_key"]!=nil{
				systemEnumMap[item["enum_field"].(string)+item["enum_key"].(string)]=item["enum_value"]
			}

		}


		importBuffer.WriteString("REPLACE INTO "+master_table+"(")
		var tableMeta *TableMetadata
		tableMeta=api.GetDatabaseMetadata().GetTableMeta(master_table)
		if tableMeta!=nil{
			primaryColumns:=tableMeta.GetPrimaryColumns()
			if len(primaryColumns)>0{
				tableKey=primaryColumns[0].ColumnName
				importBuffer.WriteString("`"+tableKey+"`,")
			}
		}

		//  primaryColumns []*ColumnMetadata
		for _,item:=range tableMeta.Columns{
			tableDataTypeMap[item.ColumnName]=item.DataType
		}

		// 删除已经导入的数据
		//var existsDependId string
		//if dependency_table!=""{
		//	option.Table=dependency_table
		//	data,errorMessage:= api.Select(option)
		//	lib.Logger.Error("errorMessage=%s",errorMessage)
		//	for _,item:=range data{
		//			existsDependId=item[dependTableKey].(string)
		//			api.Delete(dependency_table,existsDependId,nil)
		//			deMap:=make(map[string]interface{})
		//			deMap[dependTableKey]=existsDependId
		//			api.Delete(master_table,nil,deMap)
		//		}
		//
		//}

		templateDetailWhere := map[string]WhereOperation{}
		templateDetailWhere["template_key"] = WhereOperation{
			Operation: "eq",
			Value:     templateKey,
		}
       // 查询模板详情
		templateDetailOption := QueryOption{Wheres: templateDetailWhere, Table: "import_template_detail"}
		orders:=make(map[string]string)
		orders["col_num"]="asc"
		templateDetailOption.Orders =orders
		dataDetail, errorMessage := api.Select(templateDetailOption)
		lib.Logger.Infof("dataDetail", dataDetail)
		lib.Logger.Error("errorMessage=%s", errorMessage)


		for _, detail := range dataDetail {
			// 获取配置数据库表列名和excel列名
			var excelColName string
			excelColName=detail["column_name"].(string)
				// 从有效数据导入的第一行 拼接字段
			importBuffer.WriteString("`"+excelColName+"`,")

			}
		// 除模板外的字段
		var subFunc string
		var subFieldKey string
		var subFieldKeyValue string
		var condFieldKey string
		var condFieldKeyValue string

		var uniqueFunc string
		var uniqueFiledIndex0 int64
		var uniqueFiledIndex1 int64
		var assign_value_default_arr []string
		var assign_value_func_arr []string

		if extractParamMap!=nil{
			// obtain_from_where
			if extractParamMap["obtain_from_where"]!=nil{
				obtain_from_where_str:=extractParamMap["obtain_from_where"].(string)
				//extractParamArrStr:=operateCondJsonMap["conditionFields"].(string)
				json.Unmarshal([]byte(obtain_from_where_str), &extractParamArr)
			}
			// obtain_from_func
			if extractParamMap["obtain_from_func"]!=nil{
				subFunc=extractParamMap["obtain_from_func"].(string)
				//extractParamArrStr:=operateCondJsonMap["conditionFields"].(string)
				// field_key  cond_field_key
				subFieldKey=extractParamMap["field_key"].(string)
				condFieldKey=extractParamMap["cond_field_key"].(string)
			}
			// unique_func

			if extractParamMap["unique_func"]!=nil{
				uniqueFunc=extractParamMap["unique_func"].(string)
			}
			if extractParamMap["unique_filed_index0"]!=nil{
				uniqueFiledIndex0=mysql.InterToInt(extractParamMap["unique_filed_index0"])
			}
			if extractParamMap["unique_filed_index0"]!=nil{
				uniqueFiledIndex1=mysql.InterToInt(extractParamMap["unique_filed_index1"])
			}
			// assign_value_default  key=value
			if extractParamMap["assign_value_default"]!=nil{
				assign_value_default_str:=extractParamMap["assign_value_default"].(string)
				json.Unmarshal([]byte(assign_value_default_str), &assign_value_default_arr)
			}
			// assign_value_func  key=func(param)
			if extractParamMap["assign_value_func"]!=nil{
				assign_value_func_str:=extractParamMap["assign_value_func"].(string)
				json.Unmarshal([]byte(assign_value_func_str), &assign_value_func_arr)
			}

		}
		for _,item:=range extractParamArr{
			if item!=""{
				importBuffer.WriteString("`"+item+"`,")

			}
		}
		if tableMeta.HaveField(subFieldKey){
			importBuffer.WriteString("`"+subFieldKey+"`,")
		}
		if tableMeta.HaveField("import_batch_no"){
			importBuffer.WriteString("`import_batch_no`,")
		}

		if tableMeta.HaveField("submit_person"){
			importBuffer.WriteString("`submit_person`,")
		}
		for _,item:=range assign_value_default_arr{
			if item!=""&& strings.Contains(item,"="){
				itemKV:=strings.Split(item,"=")
				importBuffer.WriteString("`"+itemKV[0]+"`,")

			}
		}
		for _,item:=range assign_value_func_arr{
			if item!=""&& strings.Contains(item,"="){
				itemKV:=strings.Split(item,"=")
				importBuffer.WriteString("`"+itemKV[0]+"`,")

			}
		}
		importBuffer.WriteString("`create_time`)values")


       //print("importBuffer-head=",importBuffer.String())
		//xlsx,error := excelize.OpenFile("./upload/商品导入模板.xlsx")
		xlsx,error := excelize.OpenFile("./upload/"+fileHeader.Filename)
		if error!=nil{
			lib.Logger.Infof("error=",error)
			os.Remove("./upload/"+fileHeader.Filename)
			//os.Exit(1)
			return c.String(http.StatusInternalServerError, error.Error())
		}
		rows,error := xlsx.GetRows("Sheet1")
        if error!=nil{
        	lib.Logger.Error(error.Error())
		}
        if rows==nil{
			rows,_ = xlsx.GetRows("汇总表")
		}
		    var rowIndex int

		var tableMapArr []map[string]interface{}

	    	rowIndex=0
			for _, row := range rows {
				rowIndex=rowIndex+1
				if row_start>rowIndex{
					continue
				}
				//某一行的第一列必须有值 否则当前行不添加
				if row==nil || row[col_start-1]==""{
					break
				}
				if uniqueFunc!=""{
					param0:=convertToFormatDay(row[uniqueFiledIndex0])
					param1:=convertToFormatDay(row[uniqueFiledIndex1])
					// r := regexp.MustCompile("\\'(.*?)\\'\\.([\\w]+)\\((.*?)\\)")
					uniqueFuncStr:="select "+uniqueFunc+"('"+param0+"','"+param1+"') as result;"
					result,errorMessage:=api.ExecFuncForOne(uniqueFuncStr,"result")
					if errorMessage!=nil{
						lib.Logger.Error("errorMessage=",errorMessage)
					}

					if result!=""{
						continue
					}
				}
				importBuffer.WriteString("(")
				tableKeyValue=uuid.NewV4().String()
				importBuffer.WriteString("'"+tableKeyValue+"',")
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
					if col_start>colIndex{
						continue
					}
					var excelColName string
					var isDate string
					if col_end>=colIndex{
						excelColName=dataDetail[colIndex-col_start]["column_name"].(string)
						isDate = dataDetail[colIndex-col_start]["is_date"].(string)
					}

					if excelColName!="" && colIndex>=col_start{
						b:=tableMeta.HaveField(excelColName)

						if b==true{
							if condFieldKey==excelColName{
								condFieldKeyValue=colCell
							}
							if systemEnumMap!=nil&&systemEnumMap[master_table+"."+excelColName+colCell]!=nil{
								tableMap[excelColName]=systemEnumMap[master_table+"."+excelColName+colCell]
								// 从有效数据导入的第一行 拼接字段
								importBuffer.WriteString("'"+systemEnumMap[master_table+"."+excelColName+colCell].(string)+"',")
							}else{
								// 如果是时间格式  excel时间默认是到1900-01-01的天数  需要加上到19900-01-01的天数
								if isDate =="1" {
									colCell=convertToFormatDay(colCell)
								}
								if colCell=="" && tableDataTypeMap[excelColName]!="varchar" && tableDataTypeMap[excelColName]!="text"&& tableDataTypeMap[excelColName]!="datetime"&& tableDataTypeMap[excelColName]!="timestamp" &&tableDataTypeMap[excelColName]!="date" {
									tableMap[excelColName]="0"
									// 从有效数据导入的第一行 拼接字段
									importBuffer.WriteString("'0',")
								}else if (colCell=="" &&tableDataTypeMap[excelColName]=="datetime")||(colCell=="" && tableDataTypeMap[excelColName]=="timestamp") ||(colCell=="" &&tableDataTypeMap[excelColName]=="date"){
									importBuffer.WriteString("NULL,")
								} else{


									tableMap[excelColName]=colCell
									// 从有效数据导入的第一行 拼接字段
									importBuffer.WriteString("'"+colCell+"',")
								}

							}


						}
					}

				}
				if len(tableMap)>0{
					for _,item:=range extractParamArr{
						if item!=""{
							tableMap[item]=option.Wheres[master_table+"."+item].Value
							itemValue:=option.Wheres[master_table+"."+item].Value
							if itemValue!=nil{
								importBuffer.WriteString("'"+itemValue.(string)+"',")
							}

						}
					}
					if tableMeta.HaveField(subFieldKey){
						subFuncStr:="select "+subFunc+"('"+condFieldKeyValue+"') as result;"
						subFieldKeyValue,errorMessage=api.ExecFuncForOne(subFuncStr,"result")
						if errorMessage!=nil{
							lib.Logger.Error("errorMessage=%s",errorMessage.ErrorDescription)
						}
						tableMap[subFieldKey]=subFieldKeyValue
						importBuffer.WriteString("'"+subFieldKeyValue+"',")
					}
					if tableMeta.HaveField("import_batch_no"){
						tableMap["import_batch_no"]=importBatchNo
						importBuffer.WriteString("'"+importBatchNo+"',")
					}

					if tableMeta.HaveField("submit_person"){
						tableMap["submit_person"]=submitPerson
						importBuffer.WriteString("'"+submitPerson+"',")

					}

					for _,item:=range assign_value_default_arr{
						if item!=""&& strings.Contains(item,"="){
							itemKV:=strings.Split(item,"=")
							importBuffer.WriteString("'"+itemKV[1]+"',")

						}
					}
					for _,item:=range assign_value_func_arr{
						if item!=""&& strings.Contains(item,"="){
							itemKV:=strings.Split(item,"=")
							// 如果是 key=func(param)
							if strings.Contains(itemKV[1],"("){
								itemVArr:=strings.Split(itemKV[1],"(")
								itemVparamKeys:=strings.Replace(itemVArr[1],")","",-1)
								itemVparamKeysArr:=strings.Split(itemVparamKeys,",")
								params:=mysql.ConcatObjectProperties(itemVparamKeysArr,tableMap)
								var execSql string
								if params=="''"{
									execSql="select "+itemVArr[0]+"() as result;"
								}else{
									execSql="select "+itemVArr[0]+"("+params+") as result;"
								}

								result,errorMessage:=api.ExecFuncForOne(execSql,"result")
								if errorMessage!=nil{
									lib.Logger.Error("errorMessage=%",errorMessage)
								}
								tableMap[itemKV[0]]=result
								importBuffer.WriteString("'"+result+"',")

							}else{// 是字段赋值
								tableMap[itemKV[0]]=tableMap[itemKV[1]]
								importBuffer.WriteString("'"+mysql.InterToStr(tableMap[itemKV[1]])+"',")

							}

						}
					}

					createTime:=time.Now().Format("2006-01-02 15:04:05")
					tableMap["create_time"]=createTime
					if rowIndex==len(rows) || rows[rowIndex]==nil || rows[rowIndex][col_start-1]==""{
						importBuffer.WriteString("'"+createTime+"');")
					}else{
						importBuffer.WriteString("'"+createTime+"'),")
						totalCount=totalCount+1
					}

					//
					//_,errorMessage:=api.Create(tableName,tableMap)
					//lib.Logger.Error("errorMessage=%s",errorMessage)
					//var optionEvent QueryOption
					//optionEvent.ExtendedMap=tableMap

					// mysql.PostEvent(api,tableName,"POST",nil,optionEvent,"")
				}
				tableMapArr=append(tableMapArr,tableMap)
			}
		lib.Logger.Info("import-sql=",importBuffer.String())
		rs,error:=api.ExecSqlWithTx(importBuffer.String(),tx)
		if error!=nil{
			tx.Rollback()
			return c.String(http.StatusInternalServerError, error.Error())
		}
		lib.Logger.Info("importRs=",rs)
		// 同步任务
		var optionEvent QueryOption
		tableMap:=make(map[string]interface{})
		tableMap["import_batch_no"]=importBatchNo
		optionEvent.ExtendedMap=tableMap
		optionEvent.ExtendedArr=tableMapArr
		_,errorMessage=mysql.PostEvent(api,tx,master_table,"POST",nil,optionEvent,"")
		if errorMessage!=nil{
			tx.Rollback()
			return c.String(http.StatusInternalServerError, errorMessage.ErrorDescription)
		}
		tx.Commit()
        //  异步任务
		c1 := make (chan int);
		go asyncImportBatch(api,templateKey,master_table,importBatchNo,c1)
		// 清除上传的文件
		os.Remove("./upload/"+fileHeader.Filename)
		if errorMessage!=nil{
			return c.String(http.StatusInternalServerError, errorMessage.Error())
		}
		return c.String(http.StatusOK, strconv.Itoa(totalCount))
	}
}// api adapter.IDatabaseAPI,where string,asyncKey string,c chan int

// excel日期字段格式化 yyyy-mm-dd
func convertToFormatDay(excelDaysString string)string {
	if excelDaysString==""{
		return ""
	}
	// 正则过滤掉 非时间
	r := regexp.MustCompile("[\\d]")
	arr:=r.FindStringSubmatch(excelDaysString)
	if len(arr)<=0{
		return excelDaysString
	}
	println("excelDaysString",excelDaysString)
	// 2006-01-02 距离 1900-01-01的天数
	baseDiffDay := 38719 //在网上工具计算的天数需要加2天，什么原因没弄清楚
	curDiffDay := excelDaysString
	b, _ := strconv.Atoi(curDiffDay)
	// 获取excel的日期距离2006-01-02的天数
	realDiffDay := b - baseDiffDay
	//fmt.Println("realDiffDay:",realDiffDay)
	// 距离2006-01-02 秒数
	realDiffSecond := realDiffDay * 24 * 3600
	//fmt.Println("realDiffSecond:",realDiffSecond)
	// 2006-01-02 15:04:05距离1970-01-01 08:00:00的秒数 网上工具可查出
	baseOriginSecond := 1136185445
	println("second",int64(baseOriginSecond+realDiffSecond))
	resultTime := time.Unix(int64(baseOriginSecond+realDiffSecond), 0).Format("2006-01-02")
	println("resultTime",resultTime)
	return resultTime
}
func asyncImportBatch(api adapter.IDatabaseAPI,templateKey string,tableName string,importBatchNo string,c chan int){
	//tableName:=strings.Replace(templateKey,"_template","",-1)
	var optionEvent QueryOption
	tableMap:=make(map[string]interface{})
	tableMap["import_batch_no"]=importBatchNo
	optionEvent.ExtendedMap=tableMap

	//mysql.AsyncEvent(api,tableName,"POST",nil,optionEvent,"")
	asyncOptionEvent(api,tableName,"POST",optionEvent,c,GenerateRangeNum(800,1000))
}
func GenerateRangeNum(min, max int) int {
	rand.Seed(time.Now().Unix())
	randNum := rand.Intn(max - min) + min
	return randNum
}
func asyncOptionEvent(api adapter.IDatabaseAPI,tableName string,apiMethod string,optionEvent QueryOption,c chan int,chValue int){
	mysql.AsyncEvent(api,tableName,apiMethod,nil,optionEvent,"")
	c <- chValue
}
func asyncOptionArrEvent(api adapter.IDatabaseAPI,tableName string,apiMethod string,optionArr QueryOption,c chan int,chValue int){
	mysql.AsyncEventArr(api,tableName,apiMethod,nil,optionArr,"")
	c <- chValue
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
func endpointTableColumnCreate(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		//sql:="alter table test1 add  id_test varchar(128) comment 'id_test' comment '测试表';"

		payload, errorMessage := bodyMapOf(c)
		if errorMessage!=nil{
			lib.Logger.Error("errorMessage=%s",errorMessage)
			return c.String(http.StatusBadRequest, "error")
		}
		lib.Logger.Error("errorMessage=%s",errorMessage)
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
			lib.Logger.Error("errorMessage=%s",errorMessage)
		}
		api.UpdateAPIMetadata()
		return c.String(http.StatusOK, "ok")
	}
}
func endpointTableStructorCreate(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		//sql:="create table test1( id varchar(128) comment 'id',pass varchar(128) comment '密码') comment '测试表';"

		payload, errorMessage := bodyMapOf(c)
		if errorMessage!=nil{
			lib.Logger.Error("errorMessage=%s",errorMessage)
			return c.String(http.StatusBadRequest, "error")
		}
		lib.Logger.Error("errorMessage=%s",errorMessage)
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
			lib.Logger.Infof("tableFields=",tableFields)
			lib.Logger.Infof("detailSql=",detailSql)
			errorMessage=api.CreateTableStructure(detailSql)
			if errorMessage!=nil{
				api.Delete("report_template_config",tableName,nil)
				api.CreateTableStructure("drop table if exists "+tableName+"_detail;")
				return c.String(http.StatusInternalServerError, errorMessage.Error())
			}
		}


		errorMessage=api.CreateTableStructure(sql)
		if errorMessage!=nil{
			lib.Logger.Error("errorMessage=%s",errorMessage)
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
			lib.Logger.Error("errorMessage=%s",errorMessage)
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


func endpointTableAsync(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		asyncKey := c.QueryParam(key.ASYNC_KEY)
		lib.Logger.Infof("asyncKey=",asyncKey)
		where := c.QueryParam(key.KEY_QUERY_WHERE)
		option ,errorMessage:= parseWhereParams(where)
		lib.Logger.Infof("option=",option)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}

		 c1 := make (chan int);
		 go asyncCalculete(api,where,asyncKey,c1)

		return c.String(http.StatusOK, "ok")
	}
}
// endpointTableAsyncBatch
func endpointTableAsyncBatch(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		c1 := make (chan int);
		arr:=[5]int{1,2,3,4,5}
		for _,item:=range arr{
			go asyncFunc(item,item,c1)
		}


		return c.String(http.StatusOK, "ok")
	}
}
func endpointTableUpdateSpecificField(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		tx,error:=api.Connection().Begin()
		lib.Logger.Infof("error=",error)
		payload, errorMessage := bodyMapOf(c)
		tableName := c.Param("table")

		//lib.Logger.Infof("option=",option)
		where := c.QueryParam("where")
		option ,errorMessage:= parseWhereParams(where)
		lib.Logger.Infof("option=",option)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		meta:=api.GetDatabaseMetadata().GetTableMeta(tableName)
		cookie,err := c.Request().Cookie("Authorization")
		lib.Logger.Infof("err=",err)

		if err!=nil{
			lib.Logger.Error("errorMessage=%s",err.Error())
		}
		var jwtToken string
		if cookie!=nil{
			jwtToken=  cookie.Value
		}
		userIdJwtStr:=util.ObtainUserByToken(jwtToken,"userId")
		if meta.HaveField("update_person"){
			payload["update_person"]=userIdJwtStr
		}

		pool := newPool(redisHost,redisPassword)
		redisConn := pool.Get()
		defer redisConn.Close()
		paramBytes,err:=json.Marshal(payload)
		//lib.Logger.Error("extract paylod err=",err.Error())
		params:=string(paramBytes[:])
		paramV:=util.GetMd5String(params,true,false)

		optionParams,err:=json.Marshal(option)
		//lib.Logger.Error("extract paylod err=",err.Error())
		optionParamsArrStr:=string(optionParams[:])
		optionParamStr:=util.GetMd5String(optionParamsArrStr,true,false)

		paramVCache, errC:= redis.String(redisConn.Do("GET", paramV+optionParamStr+tableName+"PATCHWHERE"))
		if errC!=nil{
			lib.Logger.Error("obtain fom cache err=",errC.Error())
		}
		lib.Logger.Info("paramV",paramV)
		lib.Logger.Info("paramVC",paramVCache)

		if errC==nil &&paramV==paramVCache{//errC==nil&&len(paramVCache)>0 &&paramV==paramVCache[0]
			errorMessage = &ErrorMessage{ERR_REPEAT_SUBMIT, tableName+"操作重复提交!"}
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}


		rs,error:=api.UpdateBatchWithTx(tx,tableName, option.Wheres, payload)
		if error!=nil{
			tx.Rollback()
			return c.String(http.StatusInternalServerError, error.Error())
		}
		var option2 QueryOption
		rowesAffected, err := rs.RowsAffected()
		var firstPrimaryKey string
		masterTableName:=tableName
		tableMetadata:=api.GetDatabaseMetadata().GetTableMeta(masterTableName)
		var primaryColumns []*ColumnMetadata
		if tableMetadata!=nil{
			primaryColumns=tableMetadata.GetPrimaryColumns() //  primaryColumns []*ColumnMetadata
		}
		if len(primaryColumns)>0{
			firstPrimaryKey=primaryColumns[0].ColumnName
		}
		if rowesAffected >0{
			//var option QueryOption
			var arr []map[string]interface{}
			arr=append(arr,payload)
			option.ExtendedArr=arr
			var option0 QueryOption
			var masterPrimaryKeyValue string

			option0.Wheres=option.Wheres
			option0.Table=tableName
			slaveInfo,errorMessage:=api.Select(option0)
			lib.Logger.Error("errorMessage=%s",errorMessage)
			if len(slaveInfo)>0{
				masterPrimaryKeyValue=slaveInfo[0][firstPrimaryKey].(string)
			}else{
				masterPrimaryKeyValue=option.Wheres[tableName+"."+firstPrimaryKey].Value.(string)
			}

			var option1 QueryOption
			where1:=make(map[string]WhereOperation)

			where1[firstPrimaryKey]=WhereOperation{
				Operation:"eq",
				Value:masterPrimaryKeyValue,
			}
			option1.Wheres=where1
			option1.Table=tableName
			masterInfo,errorMessage:=api.Select(option1)

			var extendMap map[string]interface{}
			if len(masterInfo)>0{
				masterPrimaryKeyValue=masterInfo[0][firstPrimaryKey].(string)
				extendMap=masterInfo[0]
			}
			extendMap=mysql.BuildMapFromObj(payload,extendMap)
			option.ExtendedMap=extendMap
			option2=option
			_,errorMessage=mysql.PostEvent(api,tx,tableName,"PATCH",nil,option,"")
			if errorMessage!=nil{
				tx.Rollback()
				return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
			}


		}
		tx.Commit()
		//请求数据存在缓存中 用于校验重复提交问题
		redisConn.Do("SET", paramV+optionParamStr+tableName+"PATCHWHERE",paramV)
		// 设置有效期为1秒
		redisConn.Do("EXPIRE",paramV+optionParamStr+tableName+"PATCHWHERE",1)
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+err.Error()})
		}
		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"*"
		if strings.Contains(tableName,"detail"){
			endIndex:=strings.LastIndex(tableName,"detail")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}

		val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

		fmt.Println(val, err)
		//redisConn.Send("MULTI")
		if rowesAffected>0{
			for i, _ := range val {
				_, err = redisConn.Do("DEL", val[i])
				if err != nil {
					fmt.Println("redis delelte failed:", err)
				}
				lib.Logger.Infof("DEL-CACHE",val[i], err)
			}
		}

       //tx.Commit()
		c1 := make (chan int);
		go asyncOptionEvent(api,tableName,"PATCH",option2,c1,GenerateRangeNum(1000,1200))
		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointTableFlush(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		//tx,error:=api.Connection().Begin()
        api.UpdateAPIMetadata()
		//tx.Commit()
		return c.String(http.StatusOK, "flush table structure ok")
	}
}


func endpointTableUpdateSpecific(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		tx,error:=api.Connection().Begin()
		lib.Logger.Infof("error",error)
		var option2 QueryOption
		payload, errorMessage := bodyMapOf(c)
		tableName := c.Param("table")
		id := c.Param("id")
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}

		meta:=api.GetDatabaseMetadata().GetTableMeta(tableName)
		if meta.HaveField("update_time"){
			payload["update_time"]=time.Now().Format("2006-01-02 15:04:05")
		}
		cookie,err := c.Request().Cookie("Authorization")
		if err!=nil{
			lib.Logger.Error("errorMessage=%s",err.Error())
		}
		var jwtToken string
		if cookie!=nil{
			jwtToken=  cookie.Value
		}
		userIdJwtStr:=util.ObtainUserByToken(jwtToken,"userId")

		lib.Logger.Infof("userIdJwtStr=",userIdJwtStr)
		if meta.HaveField("update_person"){
			payload["update_person"]=userIdJwtStr
		}

		pool := newPool(redisHost,redisPassword)
		redisConn := pool.Get()
		defer redisConn.Close()
		paramBytes,err:=json.Marshal(payload)
		//lib.Logger.Error("extract paylod err=",err.Error())
		params:=string(paramBytes[:])
		paramV:=util.GetMd5String(params,true,false)
		paramVCache, errC:= redis.String(redisConn.Do("GET", paramV+id+tableName+"PATCH"))
		if errC!=nil{
			lib.Logger.Error("obtain fom cache err=",errC.Error())
		}
		lib.Logger.Info("paramV",paramV)
		lib.Logger.Info("paramVC",paramVCache)

		if errC==nil &&paramV==paramVCache{//errC==nil&&len(paramVCache)>0 &&paramV==paramVCache[0]
			errorMessage = &ErrorMessage{ERR_REPEAT_SUBMIT, tableName+"操作重复提交!"}
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}


		// 修改之前的信息
		beforeUpdateMap:=make(map[string]interface{})
		var beforeUpdateption QueryOption
		beforeWhere:=make(map[string]WhereOperation)

		var firstPrimaryKey string
		masterTableName:=tableName //strings.Replace(tableName,"_detail","",-1)
		tableMetadata:=api.GetDatabaseMetadata().GetTableMeta(masterTableName)
		var primaryColumns []*ColumnMetadata
		if tableMetadata!=nil{
			primaryColumns=tableMetadata.GetPrimaryColumns() //  primaryColumns []*ColumnMetadata
		}else{
			masterTableName=tableName
			primaryColumns=api.GetDatabaseMetadata().GetTableMeta(masterTableName).GetPrimaryColumns()
		}

		if len(primaryColumns)>0{
			for _, col := range primaryColumns {
				if col.Key == "PRI" {
					firstPrimaryKey=col.ColumnName
					lib.Logger.Infof("priId",firstPrimaryKey)
					break;//取第一个主键
				}
			}


		}
		beforeWhere[firstPrimaryKey]=WhereOperation{
			Operation:"eq",
			Value:id,
		}
		beforeUpdateption.Wheres=beforeWhere
		beforeUpdateption.Table=tableName
		beforeUpdateObj,errorMessage:=api.Select(beforeUpdateption)
		lib.Logger.Error("errorMessage=%s",errorMessage)
		if len(beforeUpdateObj)>0{
			beforeUpdateMap=beforeUpdateObj[0]
		}
		var option QueryOption
		var postOption QueryOption
		var extendMap map[string]interface{}
		extendMap=payload
		option.PriKey=firstPrimaryKey
		extendMap[firstPrimaryKey]=id
		option.ExtendedMap=extendMap
		option.ExtendedMapSecond=beforeUpdateMap
		data,errorMessage:=mysql.PreEvent(api,tableName,"PATCH",nil,option,"")
		if len(data)>0{
			payload=data[0]
		}


		//修改时不能修改主键值
		delete(payload, firstPrimaryKey)
		rs,error:=api.UpdateWithTx(tx,tableName, id, payload)
		//重新赋值给主键
		extendMap[firstPrimaryKey]=id
		option.ExtendedMap=extendMap
		postOption=option
		//fmt.Print("sql",sql)
		//rs,error:=tx.Exec(sql)
		//tx.Commit()
		//rs, errorMessage := api.Update(tableName, id, payload)
		if error != nil {
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,error.Error()}
			tx.Rollback()
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
				var option0 QueryOption
			where0:=make(map[string]WhereOperation)
			var masterPrimaryKeyValue string
			where0[firstPrimaryKey]=WhereOperation{
				Operation:"eq",
				Value:id,
			}
			option0.Wheres=where0
			option0.Table=tableName
			slaveInfo,errorMessage:=api.Select(option0)
			lib.Logger.Error("errorMessage=%s",errorMessage)
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


			if len(masterInfo)>0{
				masterPrimaryKeyValue=masterInfo[0][firstPrimaryKey].(string)
				extendMap=masterInfo[0]
			}
			option.PriKey=firstPrimaryKey
			extendMap[firstPrimaryKey]=id
			extendMap=mysql.BuildMapFromObj(payload,extendMap)
			option.ExtendedMap=extendMap
			option.ExtendedMapSecond=beforeUpdateMap
			if cookie!=nil{
				option.Authorization=cookie.Value
				postOption.Authorization=cookie.Value
			}

			option2=option
			_,errorMessage=mysql.PostEvent(api,tx,tableName,"PATCH",nil,postOption,"")
			if errorMessage!=nil{
				tx.Rollback()
				return c.String(http.StatusInternalServerError, errorMessage.ErrorDescription)
			}

		}
		tx.Commit()
		//请求数据存在缓存中 用于校验重复提交问题
		redisConn.Do("SET", paramV+id+tableName+"PATCH",paramV)
		// 设置有效期为1秒
		redisConn.Do("EXPIRE",paramV+id+tableName+"PATCH",1)

		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"*"
		if strings.Contains(tableName,"detail"){
			endIndex:=strings.LastIndex(tableName,"detail")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}

		val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))

		fmt.Println(val, err)
		//redisConn.Send("MULTI")
		if rowesAffected>0{
			for i, _ := range val {
				_, err = redisConn.Do("DEL", val[i])
				if err != nil {
					fmt.Println("redis delelte failed:", err)
				}
				lib.Logger.Infof("DEL-CACHE",val[i], err)
			}
		}

		c1 := make (chan int);
		go asyncOptionEvent(api,tableName,"PATCH",option2,c1,GenerateRangeNum(1200,1400))
		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointTableDelete(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
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
			pool:=newPool(redisHost,redisPassword)
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
				lib.Logger.Infof("DEL-CACHE",val[i], err)
			}
		}

		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointTableDeleteSpecific(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		tx,error:=api.Connection().Begin()
		lib.Logger.Error("error=",error)
		tableName := c.Param("table")
		id := c.Param("id")

		isInject:=util.ValidSqlInject(id)
		if isInject{
			errorMessage:= &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("bad param")}
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		var option QueryOption
		var ids []string
		ids=append(ids,id)
		option.Ids=ids
		// 前置事件
		_,errorMessage:=mysql.PreEvent(api,tableName,"DELETE",nil,option,"")

       //tx,error:=api.Connection().Begin()
       //fmt.Print("error",error)
       meta:=api.GetDatabaseMetadata().GetTableMeta(tableName)

		primaryColumns:=meta.GetPrimaryColumns()
		var priId string
		var priKey string
		for _, col := range primaryColumns {
			if col.Key == "PRI" {
				priKey=col.ColumnName
				lib.Logger.Infof("priId",priId)
				break;//取第一个主键
			}
		}

		whereOptionExtend := map[string]WhereOperation{}
		whereOptionExtend[priKey]=WhereOperation{
			Operation:"eq",
			Value:id,
		}// rating_status


		querOption0 := QueryOption{Wheres: whereOptionExtend, Table: tableName}
		rsQuery0, errorMessage:= api.Select(querOption0)
		for _,item:=range rsQuery0{
			lib.Logger.Infof("item=",item)
			option.ExtendedMap=item
			break
		}

        rs,error:=api.DeleteWithTx(tx,tableName, id, nil)
		//fmt.Print("sql",sql)
		//rs,error:=tx.Exec(sql)
		//rs, errorMessage := api.Delete(tableName, id, nil)
		if error != nil {
			errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,error.Error()}
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}
		rowesAffected, err := rs.RowsAffected()
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,ErrorMessage{ERR_SQL_RESULTS,"Can not get rowesAffected:"+err.Error()})
		}
		// 后置事件

		_,errorMessage=mysql.PostEvent(api,tx,tableName,"DELETE",nil,option,"")
		if errorMessage!=nil{
			tx.Rollback()
			return c.String(http.StatusInternalServerError, errorMessage.ErrorDescription)
		}else{
			tx.Commit()
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
			pool:=newPool(redisHost,redisPassword)
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

					lib.Logger.Infof("DEL-CACHE",val[i], err)
				}

			}
		}
		//tx.Commit()

		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}
// endpointBatchPut
func endpointBatchPut(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		tx,error:=api.Connection().Begin()
		lib.Logger.Error("error",error)
		payload, errorMessage := bodySliceOf(c)
		tableName := c.Param("table")
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		meta:=api.GetDatabaseMetadata().GetTableMeta(tableName)
		primaryColumns:=meta.GetPrimaryColumns()

		pool := newPool(redisHost,redisPassword)
		redisConn := pool.Get()
		defer redisConn.Close()
		paramBytes,err:=json.Marshal(payload)
		//lib.Logger.Error("extract paylod err=",err.Error())
		params:=string(paramBytes[:])
		paramV:=util.GetMd5String(params,true,false)
		paramVCache, errC:= redis.String(redisConn.Do("GET", paramV+tableName+"BATCHBATCH"))
		if errC!=nil{
			lib.Logger.Error("obtain fom cache err=",errC.Error())
		}
		lib.Logger.Info("paramV",paramV)
		lib.Logger.Info("paramVC",paramVCache)

		if errC==nil &&paramV==paramVCache{//errC==nil&&len(paramVCache)>0 &&paramV==paramVCache[0]
			errorMessage = &ErrorMessage{ERR_REPEAT_SUBMIT, tableName+"批量操作重复提交!"}
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}

		var priId string
		var priKey string
		for _, col := range primaryColumns {
			if col.Key == "PRI" {
				priKey=col.ColumnName

				lib.Logger.Infof("priId",priId)
				break;//取第一个主键
			}
		}


		var totalRowesAffected int64=0

		r_msg:=[]string{}
		cookie,err := c.Request().Cookie("Authorization")
		if err!=nil{
			lib.Logger.Error("errorMessage=%s",err.Error())
		}
		var jwtToken string
		if cookie!=nil{
			jwtToken=  cookie.Value
		}
		userIdJwtStr:=util.ObtainUserByToken(jwtToken,"userId")
		var option0 QueryOption
		var extendedArr    []map[string]interface{}
		for _, record := range payload {
			recordItem:=record.(map[string]interface{})
			var option QueryOption
			if (recordItem[priKey]==nil || (recordItem[priKey]!=nil && recordItem[priKey].(string)=="")){// 没有主键值 则是添加
				uuid := uuid.NewV4()
				priId=uuid.String()
				recordItem[priKey]=priId
				option.ExtendedMap=recordItem
				option.PriKey=priKey
				data,_:=mysql.PreEvent(api,tableName,"POST",nil,option,"")
				if len(data)>0{
					recordItem=data[0]
				}
				if meta.HaveField("create_time"){
					recordItem["create_time"]=time.Now().Format("2006-01-02 15:04:05")
				}

				if meta.HaveField("submit_person"){
					recordItem["submit_person"]=userIdJwtStr
				}
				rs,error:=api.CreateWithTx(tx,tableName, recordItem)
				//fmt.Print("sql",sql)
				//rs,error:=tx.Exec(sql)
				//fmt.Print("error",error)
				//rs, errorMessage := api.Create(tableName, recordItem)
				// 如果插入失败回滚
				if error!=nil{
					tx.Rollback()
					r_msg=append(r_msg,error.Error())
					return echo.NewHTTPError(http.StatusInternalServerError,error.Error())

				}
				rowesAffected, err := rs.RowsAffected()
				// 后置事件
				if rowesAffected>0{
					_,errorMessage=mysql.PostEvent(api,tx,tableName,"POST",nil,option,redisHost)
					extendedArr=append(extendedArr,option.ExtendedMap)
					if errorMessage!=nil{
						tx.Rollback()
						return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
					}
				}

				if err != nil {
					r_msg=append(r_msg,err.Error())
				} else {
					totalRowesAffected+=1
				}
			}else{
				priId=recordItem[priKey].(string)
				option.ExtendedMap=recordItem
				option.PriKey=priKey
				data,_:=mysql.PreEvent(api,tableName,"PATCH",nil,option,"")
				if len(data)>0{
					recordItem=data[0]
				}
				if meta.HaveField("update_person"){
					recordItem["update_person"]=userIdJwtStr
				}
				rs, error := api.UpdateWithTx(tx,tableName,priId, recordItem)
				// 如果插入失败回滚
				if error!=nil{
					r_msg=append(r_msg,error.Error())
					tx.Rollback()
					return echo.NewHTTPError(http.StatusInternalServerError,error.Error())
				}
				rowesAffected, err := rs.RowsAffected()
				// 后置事件
				if rowesAffected>0{
					_,errorMessage=mysql.PostEvent(api,tx,tableName,"PATCH",nil,option,redisHost)
					extendedArr=append(extendedArr,option.ExtendedMap)
					if errorMessage!=nil{
						tx.Rollback()
						return echo.NewHTTPError(http.StatusInternalServerError,errorMessage.ErrorDescription)
					}
				}

				if err != nil {
					r_msg=append(r_msg,err.Error())
				} else {
					totalRowesAffected+=1
				}
			}



		}


		tx.Commit()

		//请求数据存在缓存中 用于校验重复提交问题
		redisConn.Do("SET", paramV+tableName+"BATCHBATCH",paramV)
		// 设置有效期为1秒
		redisConn.Do("EXPIRE",paramV+tableName+"BATCHBATCH",1)

		option0.ExtendedArr=extendedArr
		c1 := make (chan int);

		go asyncOptionArrEvent(api,tableName,"PATCH",option0,c1,GenerateRangeNum(1400,2000))
		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"*"
		if strings.Contains(tableName,"related"){
			endIndex:=strings.LastIndex(tableName,"related")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}

		if(redisHost!=""){
			pool:=newPool(redisHost,redisPassword)
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
				lib.Logger.Infof("DEL-CACHE",val[i], err)
			}
		}
		if len(r_msg)>0{
			return c.JSON(http.StatusInternalServerError, &map[string]interface{}{"rowesAffected":totalRowesAffected,"error": r_msg})
		}
		//tx.Commit()
		return c.JSON(http.StatusOK, totalRowesAffected)
	}
}

func endpointBatchCreate(api adapter.IDatabaseAPI,redisHost string,redisPassword string) func(c echo.Context) error {
	return func(c echo.Context) error {
		tx,error:=api.Connection().Begin()
		lib.Logger.Error(error)
		payload, errorMessage := bodySliceOf(c)
		tableName := c.Param("table")
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		meta:=api.GetDatabaseMetadata().GetTableMeta(tableName)
		primaryColumns:=meta.GetPrimaryColumns()

		cookie,err := c.Request().Cookie("Authorization")
		if err!=nil{
			lib.Logger.Error("errorMessage=%s",err.Error())
		}
		var jwtToken string
		if cookie!=nil{
			jwtToken=  cookie.Value
		}
		userIdJwtStr:=util.ObtainUserByToken(jwtToken,"userId")

		lib.Logger.Infof("userIdJwtStr=",userIdJwtStr)

		pool := newPool(redisHost,redisPassword)
		redisConn := pool.Get()
		defer redisConn.Close()
		paramBytes,err:=json.Marshal(payload)
		//lib.Logger.Error("extract paylod err=",err.Error())
		params:=string(paramBytes[:])
		paramV:=util.GetMd5String(params,true,false)
		paramVCache, errC:= redis.String(redisConn.Do("GET", paramV+tableName+"POSTBATCH"))
		if errC!=nil{
			lib.Logger.Error("obtain fom cache err=",errC.Error())
		}
		lib.Logger.Info("paramV",paramV)
		lib.Logger.Info("paramVC",paramVCache)

		if errC==nil &&paramV==paramVCache{//errC==nil&&len(paramVCache)>0 &&paramV==paramVCache[0]
			errorMessage = &ErrorMessage{ERR_REPEAT_SUBMIT, tableName+"批量操作重复提交!"}
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}


		var priId string
		var priKey string
		for _, col := range primaryColumns {
			if col.Key == "PRI" {
				priKey=col.ColumnName

				lib.Logger.Infof("priId",priId)
				break;//取第一个主键
			}
		}


		var totalRowesAffected int64=0
		r_msg:=[]string{}
		var savedIds []string
		var option0 QueryOption
		var extendedArr    []map[string]interface{}
		for _, record := range payload {
			recordItem:=record.(map[string]interface{})
			if recordItem[priKey]!=nil{
				priId=recordItem[priKey].(string)
			}else{
				uuid := uuid.NewV4()
				priId=uuid.String()
				recordItem[priKey]=priId
			}
			if meta.HaveField("create_time"){
				recordItem["create_time"]=time.Now().Format("2006-01-02 15:04:05")
			}
			if meta.HaveField("submit_person"){
				recordItem["submit_person"]=userIdJwtStr
			}

			var option QueryOption
			option.ExtendedMap=recordItem
			option.PriKey=priKey
			data,_:=mysql.PreEvent(api,tableName,"POST",nil,option,"")
			if len(data)>0{
				recordItem=data[0]
			}
			_,error:=api.CreateWithTx(tx,tableName, recordItem)
			if error!=nil{
				tx.Rollback()
				return c.String(http.StatusInternalServerError, error.Error())
			}
			//tx.Commit()
			//_, err := api.Create(tableName, recordItem)
			savedIds=append(savedIds,recordItem[priKey].(string))

			// 后置事件
			_,errorMessage=mysql.PostEvent(api,tx,tableName,"POST",nil,option,redisHost)
			extendedArr=append(extendedArr,option.ExtendedMap)
			if errorMessage!=nil{
				tx.Rollback()
				return c.String(http.StatusInternalServerError, errorMessage.ErrorDescription)
			}
			if err != nil {
				r_msg=append(r_msg,err.Error())
			} else {
				totalRowesAffected+=1
			}
		}


		option0.ExtendedArr=extendedArr

        tx.Commit()
		//请求数据存在缓存中 用于校验重复提交问题
		redisConn.Do("SET", paramV+tableName+"POSTBATCH",paramV)
		// 设置有效期为1秒
		redisConn.Do("EXPIRE",paramV+tableName+"POSTBATCH",1)

		c1 := make (chan int);
		go asyncOptionArrEvent(api,tableName,"POST",option0,c1,GenerateRangeNum(2000,3000))
		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"*"
		if strings.Contains(tableName,"related"){
			endIndex:=strings.LastIndex(tableName,"related")
			cacheTable:=string(tableName[0:endIndex])
			cacheKeyPattern="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+cacheTable+"*"
		}

		if(redisHost!=""){
			pool:=newPool(redisHost,redisPassword)
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
				lib.Logger.Infof("DEL-CACHE",val[i], err)
			}
		}
		if len(r_msg)>0{
			return c.JSON(http.StatusInternalServerError, &map[string]interface{}{"rowesAffected":totalRowesAffected,"error": r_msg})
		}

		return c.JSON(http.StatusOK, totalRowesAffected)
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
	subKey:=c.QueryParam(key.SUB_KEY)
    option.FieldsType=fieldsType
    option.SubTableKey=subKey
	//lib.Logger.Infof("groupFunc",groupFunc)
	option.GroupFunc=groupFunc
	//option.Index, option.Limit, option.Offset, option.Fields, option.Wheres, option.Links, err = parseQueryParams(c)
	option.Limit, _ = strconv.Atoi(c.QueryParam(key.KEY_QUERY_PAGESIZE))  // _limit
	option.Index, _ = strconv.Atoi(c.QueryParam(key.KEY_QUERY_PAGEINDEX)) // _skip
	option.OrWheresAndTemplate,_=strconv.Atoi(c.QueryParam(key.KEY_QUERY_OR_WHERE_AND_TEMPLATE)) // _skip
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
	option.SubTableFields = make([]string, 0)
	if queryParam[key.KEY_QUERY_FIELDS] != nil { // _fields
		for _, f := range queryParam[key.SUB_KEY_QUERY_FIELDS] {
			if (f != "") {
				option.SubTableFields = append(option.SubTableFields, f)
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
	//

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
						isInject:=util.ValidSqlInject(arr[3])
						if isInject{
							errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("bad param")}
							return
						}
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
					isInject:=util.ValidSqlInject(arr[3])
					if isInject{
						errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("bad param")}
						return
					}
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
			subWhereArr := strings.Split(sWhere, "&")
			for _,subWhere:=range subWhereArr{
				arr := oswr.FindStringSubmatch(subWhere)
				if len(arr) == 4 {
					isInject:=util.ValidSqlInject(arr[3])
					if isInject{
						errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("bad param")}
						return
					}
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
	}





	osawr := regexp.MustCompile("\\'(.*?)\\'\\.([\\w]+)\\((.*?)\\)")
	if queryParam[key.KEY_QUERY_OR_WHERE_AND] != nil {
		option.OrWheresAnd = make(map[string]WhereOperation)
		for index, sWhere := range queryParam[key.KEY_QUERY_OR_WHERE_AND] {
			sWhere = strings.Replace(sWhere, "\"", "'", -1) // replace "
			subWhereArr := strings.Split(sWhere, "&")
			for _,subWhere:=range subWhereArr{
				arr := osawr.FindStringSubmatch(subWhere)
				if len(arr) == 4 {
					isInject:=util.ValidSqlInject(arr[3])
					if isInject{
						errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("bad param")}
						return
					}
					switch arr[2] {
					case "in", "notIn":
						option.OrWheresAnd[arr[1]+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], strings.Split(arr[3], ",")}
					case "like", "is", "neq", "isNot", "eq":
						option.OrWheresAnd[arr[1]+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], arr[3]}
					case "lt":
						option.OrWheresAnd[arr[1]+".lt"+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], arr[3]}
					case  "gt":
						option.OrWheresAnd[arr[1]+".gt"+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], arr[3]}
					case "lte":
						option.OrWheresAnd[arr[1]+".lte"+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], arr[3]}
					case  "gte":
						option.OrWheresAnd[arr[1]+".gte"+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], arr[3]}
					}
				}
			}



		}
	}




   // andWhereOr
	andwor := regexp.MustCompile("\\'(.*?)\\'\\.([\\w]+)\\((.*?)\\)")
	if queryParam[key.KEY_QUERY_AND_WHERE_OR] != nil {
		option.AndWheresOr = make(map[string]WhereOperation)
		for index, sWhere := range queryParam[key.KEY_QUERY_AND_WHERE_OR] {
			sWhere = strings.Replace(sWhere, "\"", "'", -1) // replace "
			subWhereArr := strings.Split(sWhere, "&")
			for _,subWhere:=range subWhereArr{
				arr := andwor.FindStringSubmatch(subWhere)
				if len(arr) == 4 {
					isInject:=util.ValidSqlInject(arr[3])
					if isInject{
						errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("bad param")}
						return
					}
					switch arr[2] {
					case "in", "notIn":
						option.AndWheresOr[arr[1]+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], strings.Split(arr[3], ",")}
					case "like", "is", "neq", "isNot", "eq":
						option.AndWheresOr[arr[1]+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], arr[3]}
					case "lt":
						option.AndWheresOr[arr[1]+".lt"+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], arr[3]}
					case  "gt":
						option.AndWheresOr[arr[1]+".gt"+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], arr[3]}
					case "lte":
						option.AndWheresOr[arr[1]+".lte"+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], arr[3]}
					case  "gte":
						option.AndWheresOr[arr[1]+".gte"+"$"+strconv.Itoa(index)] = WhereOperation{arr[2], arr[3]}
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
func parseWheres(whereStrArr []string) (option QueryOption, errorMessage *ErrorMessage) {
	option = QueryOption{}
	option.Wheres = make(map[string]WhereOperation)
   for _,whereStr:=range whereStrArr{
	   r := regexp.MustCompile("\\'(.*?)\\'\\.([\\w]+)\\((.*?)\\)")
	   if whereStr != "" {

		   sWhere:=whereStr
		   sWhere = strings.Replace(sWhere, "\"", "'", -1) // replace "
		   sWhere=strings.Replace(sWhere,"%22","'",-1)
		   // 支持同一个参数字符串里包含多个条件
		   if strings.Contains(sWhere, "&") {
			   subWhereArr := strings.Split(sWhere, "&")
			   for _, subWhere := range subWhereArr {
				   arr := r.FindStringSubmatch(subWhere)
				   if len(arr) == 4 {
					   isInject:=util.ValidSqlInject(arr[3])
					   if isInject{
						   errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("bad param")}
						   return
					   }
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
				   isInject:=util.ValidSqlInject(arr[3])
				   if isInject{
					   errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("bad param")}
					   return
				   }
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
						isInject:=util.ValidSqlInject(arr[3])
						if isInject{
							errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("bad param")}
							return
						}
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
					isInject:=util.ValidSqlInject(arr[3])
					if isInject{
						errorMessage = &ErrorMessage{ERR_PARAMETER, fmt.Sprintf("bad param")}
						return
					}
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

func newPool(server string,password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     18,
		MaxActive:   50,
		IdleTimeout: 12*3600 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", password); err != nil {
				c.Close()
				return nil, err
			}
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