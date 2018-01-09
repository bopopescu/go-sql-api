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

	)

// mountEndpoints to echo server
func mountEndpoints(s *echo.Echo, api adapter.IDatabaseAPI,databaseName string,redisConn redis.Conn) {
	s.POST("/api/"+databaseName+"/related/batch/", endpointRelatedBatch(api,redisConn)).Name = "batch save related table"
	s.PATCH("/api/"+databaseName+"/related/record/", endpointRelatedPatch(api)).Name = "update related table"
	s.GET("/api/"+databaseName+"/metadata/", endpointMetadata(api)).Name = "Database Metadata"
	s.POST("/api/"+databaseName+"/echo/", endpointEcho).Name = "Echo API"
	s.GET("/api/"+databaseName+"/endpoints/", endpointServerEndpoints(s)).Name = "Server Endpoints"
	s.HEAD("/api/"+databaseName+"/metadata/", endpointUpdateMetadata(api)).Name = "从DB获取最新的元数据"
	s.GET("/api/"+databaseName+"/swagger/", endpointSwaggerJSON(api)).Name = "Swagger Infomation"
	//s.GET("/api/swagger-ui.html", endpointSwaggerUI).Name = "Swagger UI"

	s.GET("/api/"+databaseName+"/:table", endpointTableGet(api,redisConn)).Name = "Retrive Some Records"
	s.POST("/api/"+databaseName+"/:table", endpointTableCreate(api,redisConn)).Name = "Create Single Record"
	s.DELETE("/api/"+databaseName+"/:table", endpointTableDelete(api,redisConn)).Name = "Remove Some Records"

	s.GET("/api/"+databaseName+"/:table/:id", endpointTableGetSpecific(api,redisConn)).Name = "Retrive Record By ID"
	s.DELETE("/api/"+databaseName+"/:table/:id", endpointTableDeleteSpecific(api,redisConn)).Name = "Delete Record By ID"
	s.PATCH("/api/"+databaseName+"/:table/:id", endpointTableUpdateSpecific(api,redisConn)).Name = "Update Record By ID"

	s.POST("/api/"+databaseName+"/:table/batch/", endpointBatchCreate(api,redisConn)).Name = "Batch Create Records"


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
func endpointRelatedBatch(api adapter.IDatabaseAPI,redisConn redis.Conn) func(c echo.Context) error {
	return func(c echo.Context) error {
		payload, errorMessage := bodyMapOf(c)
		masterTableName:=payload["masterTableName"].(string)
		slaveTableName:=payload["slaveTableName"].(string)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}
		rowesAffected, errorMessage := api.RelatedCreate( payload)
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
		}
		cacheKeyPattern:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+masterTableName+"*"
		val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))
		fmt.Println(val, err)
		redisConn.Send("MULTI")
		for i, _ := range val {
			redisConn.Send("DEL", val[i])
		}

		cacheKeyPattern1:="/api"+"/"+api.GetDatabaseMetadata().DatabaseName+"/"+slaveTableName+"*"
		val1, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern1))
		//fmt.Println(val1, err)
		redisConn.Send("MULTI")
		for i, _ := range val1 {
			redisConn.Send("DEL", val1[i])
		}

		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
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

func endpointTableGet(api adapter.IDatabaseAPI,redisConn redis.Conn) func(c echo.Context) error {

	return func(c echo.Context) error {
		tableName := c.Param("table")
		option ,errorMessage:= parseQueryParams(c)
		option.Table = tableName

		paramBytes,err:=option.MarshalJSON()
		params:=string(paramBytes[:])
		params=strings.Replace(params,"\"","-",-1)
		params=strings.Replace(params,":","-",-1)
		params=strings.Replace(params,",","-",-1)
		params=strings.Replace(params,"{","",-1)
		params=strings.Replace(params,"}","",-1)
		params=strings.Replace(params,"-","",-1)
		params=strings.Replace(params,"null","",-1)
		params=strings.Replace(params,"[]","",-1)
		fmt.Printf("params=",params)
		cacheData, err := redis.String(redisConn.Do("GET", params))
		if err != nil {
			fmt.Println("redis get failed:", err)
		} else {
			fmt.Printf("Get mykey: %v \n", cacheData)
		}

		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
		}

		if option.Index==0{
			// 如果缓存中有值 用缓存中的值  否则把查询出来的值放在缓存中
			if cacheData!=""{
				return responseTableGet(c,cacheData,false,tableName,api,params,redisConn)
			}

			//无需分页,直接返回数组
			data, errorMessage := api.Select(option)
			if errorMessage != nil {
				return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
			}
			return responseTableGet(c,data,false,tableName,api,params,redisConn)
		}else{
			cacheTotalCount,err:=redis.String(redisConn.Do("GET",params+"-totalCount"))
			//cacheTotalCount=cacheTotalCount.(string)
			fmt.Printf("cacheTotalCount",cacheTotalCount)
			fmt.Printf("err",err)
			fmt.Printf("cacheData",cacheData)
			if cacheTotalCount!="" &&cacheData!=""&&err==nil{
				totalCount:=0
				totalCount,err:=strconv.Atoi(cacheTotalCount)
				if err!=nil{
					fmt.Printf("err",err)
				}
				return responseTableGet(c, &Paginator{int(option.Offset/option.Limit+1),option.Limit, int(math.Ceil(float64(totalCount)/float64(option.Limit))),totalCount,cacheData},true,tableName,api,params,redisConn)

			}else{
				//分页
				totalCount,errorMessage:=api.SelectTotalCount(option)
				if errorMessage != nil {
					return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
				}

				data, errorMessage := api.Select(option)
				redisConn.Do("SET",params+"-totalCount",totalCount)
				if errorMessage != nil {
					return echo.NewHTTPError(http.StatusInternalServerError,errorMessage)
				}
				return responseTableGet(c, &Paginator{int(option.Offset/option.Limit+1),option.Limit, int(math.Ceil(float64(totalCount)/float64(option.Limit))),totalCount,data},true,tableName,api,params,redisConn)

			}


			}
	}
}

func responseTableGet(c echo.Context,data interface{},ispaginator bool,filename string,api adapter.IDatabaseAPI,cacheParams string,redisConn redis.Conn) error{
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
		cacheData,err:=redis.String(redisConn.Do("GET",cacheParams))
		if err!=nil{
			fmt.Printf("err",err)
		}
		if ispaginator&&cacheData!=""{
			var paginator Paginator
			json.Unmarshal([]byte(cacheData), &paginator)
			return c.JSON( http.StatusOK,paginator)
		}else if cacheData!=""{
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
			fmt.Printf("cacheDataStr",cacheDataStr)

			redisConn.Do("SET",cacheParams,cacheDataStr)
			return c.JSON( http.StatusOK,data2)
		}else if ispaginator && len(data.(*Paginator).Data.([]map[string]interface{}))==0{
			data2:=data.(*Paginator)
			data2.Data=[]string{}
			return c.JSON( http.StatusOK,data2)
		}else {

			dataByte,err:=json.Marshal(data)
			if err!=nil{
				fmt.Printf("err",err)
			}
			cacheDataStr:=string(dataByte[:])
			fmt.Printf("cacheDataStr",cacheDataStr)

			redisConn.Do("SET",cacheParams,cacheDataStr)
			return c.JSON( http.StatusOK,data)
		}
	}
}


func endpointTableGetSpecific(api adapter.IDatabaseAPI,redisConn redis.Conn) func(c echo.Context) error {
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

			val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))
			fmt.Println(val, err)
			redisConn.Send("MULTI")
			for i, _ := range val {
				redisConn.Send("DEL", val[i])
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

func endpointTableCreate(api adapter.IDatabaseAPI,redisConn redis.Conn) func(c echo.Context) error {
	return func(c echo.Context) error {
		payload, errorMessage := bodyMapOf(c)
		tableName := c.Param("table")
		if errorMessage != nil {
			return echo.NewHTTPError(http.StatusBadRequest,errorMessage)
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

		val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))
		fmt.Println(val, err)
		redisConn.Send("MULTI")
		for i, _ := range val {
			redisConn.Send("DEL", val[i])
		}
		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointTableUpdateSpecific(api adapter.IDatabaseAPI,redisConn redis.Conn) func(c echo.Context) error {
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
		val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))
		fmt.Println(val, err)
		redisConn.Send("MULTI")
		for i, _ := range val {
			redisConn.Send("DEL", val[i])
		}
		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointTableDelete(api adapter.IDatabaseAPI,redisConn redis.Conn) func(c echo.Context) error {
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

		val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))
		fmt.Println(val, err)
		redisConn.Send("MULTI")
		for i, _ := range val {
			redisConn.Send("DEL", val[i])
		}
		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointTableDeleteSpecific(api adapter.IDatabaseAPI,redisConn redis.Conn) func(c echo.Context) error {
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

		val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))
		fmt.Println(val, err)
		redisConn.Send("MULTI")
		for i, _ := range val {
			redisConn.Send("DEL", val[i])
		}
		return c.String(http.StatusOK, strconv.FormatInt(rowesAffected,10))
	}
}

func endpointBatchCreate(api adapter.IDatabaseAPI,redisConn redis.Conn) func(c echo.Context) error {
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

		val, err := redis.Strings(redisConn.Do("KEYS", cacheKeyPattern))
		fmt.Println(val, err)
		redisConn.Send("MULTI")
		for i, _ := range val {
			redisConn.Send("DEL", val[i])
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
			arr := r.FindStringSubmatch(sWhere)
			if len(arr) == 4 {
				switch arr[2] {
				case "in", "notIn":
					option.Wheres[arr[1]] = WhereOperation{arr[2], strings.Split(arr[3], ",")}
				case "like", "is", "neq", "isNot", "eq":
					option.Wheres[arr[1]] = WhereOperation{arr[2], arr[3]}
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