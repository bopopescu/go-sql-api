package server

import (
	"net/http"
	"github.com/shiyongabc/go-mysql-api/server/swagger"
	"github.com/labstack/echo"
	"github.com/shiyongabc/go-mysql-api/server/static"
	"github.com/shiyongabc/go-mysql-api/adapter"
	. "github.com/shiyongabc/go-mysql-api/types"
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
)

// mountEndpoints to echo server
func mountEndpoints(s *echo.Echo, api adapter.IDatabaseAPI,databaseName string,redisHost string) {
	s.GET("/api/"+databaseName+"/clear/cache/", endpointTableClearCacheSpecific(api,redisHost)).Name = "clear cache"

	s.POST("/api/"+databaseName+"/related/batch/", endpointRelatedBatch(api,redisHost)).Name = "batch save related table"
	s.DELETE("/api/"+databaseName+"/related/delete/", endpointRelatedDelete(api,redisHost)).Name = "batch delete related table"
	s.PATCH("/api/"+databaseName+"/related/record/", endpointRelatedPatch(api)).Name = "update related table"
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
	s.PUT("/api/"+databaseName+"/:table/:id", endpointTableUpdateSpecific(api,redisHost)).Name = "Put Record By ID"

	s.POST("/api/"+databaseName+"/:table/batch/", endpointBatchCreate(api,redisHost)).Name = "Batch Create Records"


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

		for _,slaveInfo:=range data {
			slaveId:= slaveInfo["id"].(string)
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

func endpointTableGet(api adapter.IDatabaseAPI,redisHost string) func(c echo.Context) error {

	return func(c echo.Context) error {
		tableName := c.Param("table")
		option ,errorMessage:= parseQueryParams(c)
		option.Table = tableName

		paramBytes,err:=option.MarshalJSON()
		if err!=nil{
			fmt.Printf("err",err)
		}

		params:=string(paramBytes[:])
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

		params="/api/"+api.GetDatabaseMetadata().DatabaseName+"/"+tableName+"/"+params
		fmt.Printf("params=",params)
		var cacheData string
		if redisHost!=""{
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
				return responseTableGet(c,cacheData,false,tableName,api,params,redisHost)
			}

			//无需分页,直接返回数组
			data, errorMessage := api.Select(option)
			if errorMessage != nil {
				return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
			}
			return responseTableGet(c,data,false,tableName,api,params,redisHost)
		}else{
			var cacheTotalCount string
			if(redisHost!=""){
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
				return responseTableGet(c, &Paginator{int(option.Offset/option.Limit+1),option.Limit, int(math.Ceil(float64(totalCount)/float64(option.Limit))),totalCount,cacheData},true,tableName,api,params,redisHost)

			}else{

				//分页
				totalCount,errorMessage:=api.SelectTotalCount(option)
				if errorMessage != nil {
					return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
				}

				data, errorMessage := api.Select(option)
				if(redisHost!=""){
					pool:=newPool(redisHost)
					redisConn:=pool.Get()
					defer redisConn.Close()
					redisConn.Do("SET",params+"-totalCount",totalCount)
				}

				if errorMessage != nil {
					return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
				}
				return responseTableGet(c, &Paginator{int(option.Offset/option.Limit+1),option.Limit, int(math.Ceil(float64(totalCount)/float64(option.Limit))),totalCount,data},true,tableName,api,params,redisHost)

			}


		}
	}
}

func responseTableGet(c echo.Context,data interface{},ispaginator bool,filename string,api adapter.IDatabaseAPI,cacheParams string,redisHost string) error{
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
		if(redisHost!=""){
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

			if(redisHost!=""){
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

			if(redisHost!=""){
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
		tableName := c.Param("table")
		cacheKey := c.Param("cacheKey")

			cacheKeyPattern:=cacheKey
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
	//option.Index, option.Limit, option.Offset, option.Fields, option.Wheres, option.Links, err = parseQueryParams(c)
	option.Limit, _ = strconv.Atoi(c.QueryParam(key.KEY_QUERY_PAGESIZE))      // _limit
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
			if(f!=""){
				option.Fields = append(option.Fields, f)
			}
		}
	}
	if queryParam[key.KEY_QUERY_LINK] != nil { // _link
		option.Links = make([]string,0)
		for _, f := range queryParam[key.KEY_QUERY_LINK] {
			if(f!=""){
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
			if strings.Contains(sWhere,"&"){
				subWhereArr:=	strings.Split(sWhere,"&")
				for _,subWhere:=range subWhereArr{
					arr := r.FindStringSubmatch(subWhere)
					if len(arr) == 4 {
						switch arr[2] {
						case "in", "notIn":
							option.Wheres[arr[1]] = WhereOperation{arr[2], strings.Split(arr[3], ",")}
						case "like", "is", "neq", "isNot", "eq","lt","gt":
							option.Wheres[arr[1]] = WhereOperation{arr[2], arr[3]}
						}
					}
				}
			}else{
				arr := r.FindStringSubmatch(sWhere)
				if len(arr) == 4 {
					switch arr[2] {
					case "in", "notIn":
						option.Wheres[arr[1]] = WhereOperation{arr[2], strings.Split(arr[3], ",")}
					case "like", "is", "neq", "isNot", "eq","lt","gt":
						option.Wheres[arr[1]] = WhereOperation{arr[2], arr[3]}
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