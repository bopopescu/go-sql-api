package mysql

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strings"
	"fmt"
	"encoding/json"
	"strconv"
	"container/list"
	"github.com/shiyongabc/go-mysql-api/adapter"
	."github.com/shiyongabc/go-mysql-api/types"

)

//前置事件处理

func PreEvent(api adapter.IDatabaseAPI,tableName string ,equestMethod string,data []map[string]interface{},option QueryOption,redisHost string)(rs []map[string]interface{},errorMessage *ErrorMessage){
	operates,errorMessage:=	SelectOperaInfo(api,api.GetDatabaseMetadata().DatabaseName+"."+tableName,equestMethod)
	fmt.Printf("errorMessage=",errorMessage)
	var operate_condition string
	var operate_content string
	var conditionType string
	var conditionTable string
	var conditionFileds string
	var resultFileds string
	var operate_type string
	var operate_table string
	var operateFunc string
	var priKey string
	var actionType string
	var actionFiledArr []string
	var actionFiledArrStr string

	var filter_content string
	var filterFiledArr []string
	var filterFiledArrStr string
	var filterKey string
	var filterFunc string
	//	var actionType string
	var conditionFiledArr []string
	var resultFieldsArr []string
	//var actionFieldsArr [5]string
	var operateCondJsonMap map[string]interface{}
	var operateCondContentJsonMap map[string]interface{}
	var filterCondContentJsonMap map[string]interface{}
	fieldList:=list.New()
	var fields []string
	for _,operate:=range operates {
		operate_condition= operate["operate_condition"].(string)
		operate_content = operate["operate_content"].(string)
		filter_content=operate["filter_content"].(string)
		if(operate_condition!=""){
			json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)
			if operateCondJsonMap["conditionType"]!=nil{
				conditionType=operateCondJsonMap["conditionType"].(string)
				fmt.Printf("conditionType=",conditionType)
			}

			if operateCondJsonMap["conditionFields"]!=nil{
				conditionFileds=operateCondJsonMap["conditionFields"].(string)
			}

			if operateCondJsonMap["resultFields"]!=nil{
				resultFileds=operateCondJsonMap["resultFields"].(string)
			}
			if operateCondJsonMap["conditionTable"]!=nil{
				conditionTable=operateCondJsonMap["conditionTable"].(string)
			}

			json.Unmarshal([]byte(conditionFileds), &conditionFiledArr)
			json.Unmarshal([]byte(resultFileds), &resultFieldsArr)
		}
		if(operate_content!=""){
			json.Unmarshal([]byte(operate_content), &operateCondContentJsonMap)
		}
		for _,item:= range conditionFiledArr{
			if item!=""{
				fields=append(fields, item)
				fieldList.PushBack(item)

			}
		}

		var conditionFieldKey string
		var conditionComplex string
		if operateCondJsonMap["conditionComplex"]!=nil{
			conditionComplex=operateCondJsonMap["conditionComplex"].(string)
		}
		if operateCondJsonMap["conditionFieldKey"]!=nil{
			conditionFieldKey=operateCondJsonMap["conditionFieldKey"].(string)
		}
		if operateCondContentJsonMap["operate_func"]!=nil{
			operateFunc=operateCondContentJsonMap["operate_func"].(string)
		}
		if operateCondContentJsonMap["action_type"]!=nil{
			actionType=operateCondContentJsonMap["action_type"].(string)
		}
		if operateCondContentJsonMap["pri_key"]!=nil{
			priKey=operateCondContentJsonMap["pri_key"].(string)
		}
		if operateCondContentJsonMap["action_fields"]!=nil{
			actionFiledArrStr=operateCondContentJsonMap["action_fields"].(string)
		}
		json.Unmarshal([]byte(actionFiledArrStr), &actionFiledArr)

		var conditionFieldKeyValue string
		if strings.Contains(conditionFieldKey,"="){
			arr:=strings.Split(conditionFieldKey,"=")
			conditionFieldKey=arr[0]
			conditionFieldKeyValue=arr[1]
			fmt.Printf("conditionFieldKeyValue=",conditionFieldKeyValue)
		}

		//判断条件类型 如果是JUDGE 判断是否存在 如果存在做操作后动作
		// {"operate_type":"UPDATE","pri_key":"id","action_type":"ACC","action_field":"goods_num"}
		operate_type=operateCondContentJsonMap["operate_type"].(string)
		operate_table=operateCondContentJsonMap["operate_table"].(string)
		fmt.Printf("operate_type=",operate_type)
		fmt.Printf("operate_table=",operate_table)
		fmt.Printf("operate_type=",conditionTable)


		if(filter_content!=""){
			json.Unmarshal([]byte(filter_content), &filterCondContentJsonMap)
		}
		if filterCondContentJsonMap["filterFunc"]!=nil{
			filterFunc=filterCondContentJsonMap["filterFunc"].(string)
		}
		if filterCondContentJsonMap["filterFields"]!=nil{
			filterFiledArrStr=filterCondContentJsonMap["filterFields"].(string)
			json.Unmarshal([]byte(filterFiledArrStr), &filterFiledArr)
		}
		var isFiltered bool
		for _,item:=range filterFiledArr{
			if strings.Contains(item,"="){
				arr:=strings.Split(item,"=")
				field0:=arr[0]
				value0:=arr[1]
				if option.ExtendedMap[field0]==value0{
					isFiltered=true
					break;
				}

			}

		}
		// 如果被拦截 则不执行当前前置事件
		if isFiltered{
			continue
		}
		if filterFunc!=""{
			filterFuncSql:="select "+filterFunc+"('"+ConverStrFromMap(filterKey,option.ExtendedMap)+"') as result;"
			filterResult:=api.ExecFuncForOne(filterFuncSql,"result")
			if filterResult!=""{
				continue
			}
		}
		// filterFieldKey
		if filterCondContentJsonMap["filterFieldKey"]!=nil{
			filterKey=filterCondContentJsonMap["filterFieldKey"].(string)
		}
		// 前置事件新处理方式   只传参数   逻辑处理在存储过程处理
		if "CASCADE_DELETE"==operate_type|| "UPDATE_MASTER"==operate_type{
			if operateFunc!=""{
				ids:=option.Ids
				for _,item:=range ids{
					operateFuncSql:="select "+operateFunc+"('"+item+"');"
					_,errorMessage:=api.ExecFunc(operateFuncSql)
					fmt.Printf("errorMessage=",errorMessage)
				}


			}
		}
		// UPDATE_MASTER
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
					Value:     option.ExtendedMap[e.Value.(string)].(string),
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


			action_field_value1:=option.ExtendedMap[action_field].(float64)
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
					fmt.Printf("rowesAffected=",rowesAffected)
					if error!=nil{
						fmt.Printf("err=",error)
					}

				}

			}
		}
		if "COVER_VALUE"==operate_type{
			// remote request  API_REQUEST-POST
			if strings.Contains(actionType,"API_REQUEST"){
				client := &http.Client{}
				reqMethod:=strings.Split(actionType,"-")[1]
				// 构造请求参数
				reStr:=BuildObjectProperties(conditionFiledArr,option.ExtendedMap,actionFiledArr)
				reqest, err := http.NewRequest(reqMethod, operate_table, bytes.NewBuffer(reStr))
				if option.Authorization==""{
					fmt.Println("authorization is null")
					continue
				}
				//增加header选项
				reqest.Header.Set("Cookie", option.Authorization)
				reqest.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36")
				reqest.Header.Set("Accept", "application/json, text/plain, */*")
				reqest.Header.Set("Content-type", "application/json")
				if err != nil {
					panic(err)
				}
				//处理返回结果
				response, _ := client.Do(reqest)
				fmt.Print("response", response)
				var resultMap map[string]interface{}
				if response.StatusCode == 200 {
					body, _ := ioutil.ReadAll(response.Body)
					json.Unmarshal(body, &resultMap)
					fmt.Println("responseBody",string(body))

					option.ExtendedMap[conditionFieldKey]=resultMap[priKey]
				}
			}
			if operateFunc!=""{
				var operateFuncSql string
				params:=ConcatObjectProperties(conditionFiledArr,option.ExtendedMap)
				if params!="''"{
					operateFuncSql="select "+operateFunc+"("+params+") as result;"
				}else{
					operateFuncSql="select "+operateFunc+"() as result;"
				}

					result:=api.ExecFuncForOne(operateFuncSql,"result")
					if result!="" && conditionFieldKey!=""{
						option.ExtendedMap[conditionFieldKey]=result
					}
					fmt.Printf("errorMessage=",errorMessage)


			}
		}
		if "PRE_SYNC_COMPLEX"==operate_type{
				var conditionComplexKey,conditionComplexValue,paramComplexValue string
				if strings.Contains(conditionComplex,"=") {
					arr := strings.Split(conditionComplex, "=")
					conditionComplexKey = arr[0]
					conditionComplexValue = arr[1]

				}
			switch  option.ExtendedMap[conditionComplexKey].(type) {
			case string:
				paramComplexValue=option.ExtendedMap[conditionComplexKey].(string)
			case 	float64:
				paramComplexValue=strconv.FormatFloat(option.ExtendedMap[conditionComplexKey].(float64), 'f', -1, 64)
			}
				if paramComplexValue==conditionComplexValue{
					continue
				}
			if operateFunc!=""{
				var operateFuncSql string
				// 预处理有关联关系
				related_table:=strings.Replace(tableName,"_detail","",-1)
				var relatedOption QueryOption
				relatedOption.Table=tableName
				whereMap:= map[string]WhereOperation{}
				whereMap[tableName+"."+conditionFieldKey]=WhereOperation{
					"eq",
					option.ExtendedMap[conditionFieldKey],
									}
				whereMap[conditionComplexKey]=WhereOperation{
					"eq",
					conditionComplexValue,
				}
				relatedOption.Wheres=whereMap
				relatedOption.Links=[]string{related_table}
				fields=append(fields, "id")
				relatedOption.Fields=fields
				relatedData, errorMessage:= api.Select(relatedOption)
				fmt.Printf("errorMessage=",errorMessage)
				extendMapArr:=option.ExtendedArr
				for _,item:=range relatedData{
					for _,slaveItem:=range extendMapArr{
						if slaveItem["id"]==item["id"]{
							var overFirstValue string
							if len(conditionFiledArr)>0{
								overFirstValue=conditionFiledArr[0]
								if strings.Contains(overFirstValue,".") {
									arr := strings.Split(overFirstValue, ".")
									overFirstValue = arr[1]
								}
								item[overFirstValue]=slaveItem[overFirstValue]
							}

							continue
						}
					}
					params:=ConcatObjectProperties(conditionFiledArr,item)
					if params!="''"{
						operateFuncSql="select "+operateFunc+"("+params+") as result;"
					}else{
						operateFuncSql="select "+operateFunc+"() as result;"
					}

					result:=api.ExecFuncForOne(operateFuncSql,"result")
					if result!="" && conditionFieldKey!=""{
						option.ExtendedMap[conditionFieldKey]=result
					}
					fmt.Printf("errorMessage=",errorMessage)
				}



			}
		}
	}

	// {"conditionType":"JUDGE","conditionTable":"customer.shopping_cart","conditionFields":"[\"customer_id\",\"goods_id\"]"}
    if data== nil && option.ExtendedMap!=nil{
    	data=append(data,option.ExtendedMap)
	}
	return data,nil;
}


//后置事件处理
func PostEvent(api adapter.IDatabaseAPI,tableName string ,equestMethod string,data []map[string]interface{},option QueryOption,redisHost string)(rs []map[string]interface{},errorMessage *ErrorMessage){
	operates,errorMessage:=	SelectOperaInfo(api,api.GetDatabaseMetadata().DatabaseName+"."+tableName,equestMethod)
	fmt.Printf("errorMessage=",errorMessage)
	var operate_condition string
	var operate_content string
	var filter_content string
	var conditionType string
	var conditionTable string
	var conditionFileds string
	var resultFileds string
	var operate_type string
	var operate_table string
	var operateFunc string
	var filterFunc string
	var filterFieldKey string
	//	var actionType string
	var conditionFiledArr []string
	var resultFieldsArr []string
	var actionFieldsArr []string
	var operateCondJsonMap map[string]interface{}
	var operateCondContentJsonMap map[string]interface{}
	var operateFilterContentJsonMap map[string]interface{}
	fieldList:=list.New()

	for _,operate:=range operates {
		operate_condition= operate["operate_condition"].(string)
		operate_content = operate["operate_content"].(string)
		filter_content = operate["filter_content"].(string)

		if(operate_condition!=""){
			json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)
			if operateCondJsonMap["conditionType"]!=nil{
				conditionType=operateCondJsonMap["conditionType"].(string)
			}

			if operateCondJsonMap["conditionFields"]!=nil{
				conditionFileds=operateCondJsonMap["conditionFields"].(string)
			}

			if operateCondJsonMap["resultFields"]!=nil{
				resultFileds=operateCondJsonMap["resultFields"].(string)
			}
			if operateCondJsonMap["conditionTable"]!=nil{
				conditionTable=operateCondJsonMap["conditionTable"].(string)
			}

			json.Unmarshal([]byte(conditionFileds), &conditionFiledArr)
			json.Unmarshal([]byte(resultFileds), &resultFieldsArr)
		}
		if(operate_content!=""){
			json.Unmarshal([]byte(operate_content), &operateCondContentJsonMap)
		}
		if(filter_content!=""){
			json.Unmarshal([]byte(filter_content), &operateFilterContentJsonMap)
		}
		if operateFilterContentJsonMap["filterFunc"]!=nil{
			filterFunc=operateFilterContentJsonMap["filterFunc"].(string)
		}
		if operateFilterContentJsonMap["filterFieldKey"]!=nil{
			filterFieldKey=operateFilterContentJsonMap["filterFieldKey"].(string)
		}
		for _,item:= range conditionFiledArr{
			if item !=""{
				fieldList.PushBack(item)
			}
		}

		var conditionFieldKey string
		if operateCondJsonMap["conditionFieldKey"]!=nil{
			conditionFieldKey=operateCondJsonMap["conditionFieldKey"].(string)
		}
		if operateCondContentJsonMap["operate_func"]!=nil{
			operateFunc=operateCondContentJsonMap["operate_func"].(string)
			fmt.Printf("operateFunc=",operateFunc)
		}

		var conditionFieldKeyValue string
		if strings.Contains(conditionFieldKey,"="){
			arr:=strings.Split(conditionFieldKey,"=")
			conditionFieldKey=arr[0]
			conditionFieldKeyValue=arr[1]
		}
		if filterFieldKey=="PRIMARY"{ //如果是主键 取主键字段名
			filterFieldKey=option.PriKey
		}
		if conditionFieldKeyValue==""{
			if conditionFieldKey=="PRIMARY"{ //如果是主键 取主键字段名
				conditionFieldKey=option.PriKey
			}
			if option.ExtendedMap[conditionFieldKey]!=nil{
				conditionFieldKeyValue=option.ExtendedMap[conditionFieldKey].(string)
			}
		}
		//判断条件类型 如果是JUDGE 判断是否存在 如果存在做操作后动作
		// {"operate_type":"UPDATE","pri_key":"id","action_type":"ACC","action_field":"goods_num"}
		if operateCondContentJsonMap["operate_type"]!=nil{
			operate_type=operateCondContentJsonMap["operate_type"].(string)
		}
		if operateCondContentJsonMap["operate_table"]!=nil{
			operate_table=operateCondContentJsonMap["operate_table"].(string)
		}

		//actionType=operateCondContentJsonMap["action_type"].(string)
		// 动态添加列 并为每一列计算出值
		if "DYNAMIC_ADD_COLUMN"==operate_type {
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
		if "UPDATE"==operate_type && "QUERY"==conditionType{
			for _,item:= range conditionFiledArr{
				if item!=""{
					fieldList.PushBack(item)
				}
			}
			//  从配置里获取要判断的字段 并返回对象
			whereOption := map[string]WhereOperation{}
			for e := fieldList.Front(); e != nil; e = e.Next() {
				// 含有= 取=后面值
				if strings.Contains(e.Value.(string),"="){
					arr:=strings.Split(e.Value.(string),"=")
					whereOption[arr[0]] = WhereOperation{
						Operation: "eq",
						Value:     arr[1],
					}
				}
				if option.Wheres!=nil&&option.Wheres[e.Value.(string)].Value!=nil{
					whereOption[e.Value.(string)] = WhereOperation{
						Operation: "eq",
						Value:     option.Wheres[e.Value.(string)].Value,
					}

				}

				if option.ExtendedMap!=nil&&option.ExtendedMap[e.Value.(string)]!=nil{

					whereOption[e.Value.(string)] = WhereOperation{
						Operation: "eq",
						Value:     option.ExtendedMap[e.Value.(string)],
					}

				}

			}

			querOption := QueryOption{Wheres: whereOption, Table: tableName}
			rsQuery, errorMessage:= api.Select(querOption)
			if errorMessage!=nil{
				fmt.Printf("errorMessage", errorMessage)
			}else{
				fmt.Printf("rs", rsQuery)
			}
			//operate_type:=operateCondContentJsonMap["operate_type"].(string)
			pri_key:=operateCondContentJsonMap["pri_key"].(string)
			var pri_key_value string
			action_type:=operateCondContentJsonMap["action_type"].(string)
			action_fields:=operateCondContentJsonMap["action_fields"].(string)
			json.Unmarshal([]byte(action_fields), &actionFieldsArr)
			// 操作类型是更新 动作类型是累加
			actionFiledMap:=make(map[string]interface{})
			//	if operate_type=="UPDATE"{
			for _,field:=range actionFieldsArr{
				if strings.Contains(field,"="){
					arr:=strings.Split(field,"=")
					actionFiledMap[arr[0]]=arr[1]
				}


			}

			var conditionFieldKeyValueStr string
			if len(option.ExtendedArr)>0{
				switch  option.ExtendedArr[0][conditionFieldKey].(type) {
				case string:
					conditionFieldKeyValueStr=option.ExtendedArr[0][conditionFieldKey].(string)
				case 	float64:
					conditionFieldKeyValueStr=strconv.FormatFloat(option.ExtendedArr[0][conditionFieldKey].(float64), 'f', -1, 64)
				}
			}

			//conditionFieldKeyValueStr:=strconv.FormatFloat(option.ExtendedArr[0][conditionFieldKey].(float64), 'f', -1, 64)
			//   && (option.ExtendedMapSecond[conditionFieldKey]!=option.ExtendedArr[0][conditionFieldKey]||conditionFieldKey=="")
			if action_type=="ACC"  && (conditionFieldKeyValueStr==conditionFieldKeyValue|| option.ExtendedArr[0][conditionFieldKey]=="")&& (option.ExtendedMapSecond[conditionFieldKey]!=option.ExtendedArr[0][conditionFieldKey]||conditionFieldKey==""){
				for _,rsQ:=range rsQuery {
					pri_key_value=rsQ[pri_key].(string)
					for _,field:=range actionFieldsArr{
						if rsQ[field]!=nil{
							action_field_value0:= rsQ[field].(string)
							if action_field_value0!=""{
								action_field_value0_int,err0:=strconv.Atoi(action_field_value0)
								action_field_value0_int=action_field_value0_int+1
								if err0!=nil{
									fmt.Printf("err0",err0)
								}
								actionFiledMap[field]=action_field_value0_int
							}


						}


					}


				}
			}
			if action_type=="SUB_FROM_CONFIRM_FAIL" && conditionFieldKeyValue==conditionFieldKeyValueStr && (option.ExtendedMapSecond[conditionFieldKey]!=option.ExtendedArr[0][conditionFieldKey]||conditionFieldKey==""){

				//	fmt.Printf("option.ExtendedArr[0][conditionFieldKey]=",option.ExtendedArr[0][conditionFieldKey],",conditionFieldKeyValue=",conditionFieldKeyValue)
				for _,rsQ:=range rsQuery {
					pri_key_value=rsQ[pri_key].(string)
					for _,field:=range actionFieldsArr{
						if rsQ[field]!=nil{
							action_field_value0:= rsQ[field].(string)
							if action_field_value0!=""{
								action_field_value0_int,err0:=strconv.Atoi(action_field_value0)
								action_field_value0_int=action_field_value0_int-1
								if action_field_value0_int<0{
									action_field_value0_int=0
								}
								if err0!=nil{
									fmt.Printf("err0",err0)
								}
								actionFiledMap[field]=action_field_value0_int
							}


						}


					}


				}
			}
			if action_type=="UPDATE_RECORD"{
				updateWhere:=make(map[string]WhereOperation)
				if option.ExtendedMap[conditionFieldKey]!=nil{
					updateWhere[conditionFieldKey]=WhereOperation{
						Operation:"eq",
						Value:option.ExtendedMap[conditionFieldKey].(string),
					}

				}
				for _,field:=range actionFieldsArr{
					if strings.Contains(field,"="){
						arr:=strings.Split(field,"=")
						actionFiledMap[arr[0]]=arr[1]
					}

				}
               // 如果 updateWhere有值  则按updateWhere更新  否则是查询条件 whereOption
               if len(updateWhere)<=0{
				   updateWhere=whereOption
			   }
				rsU,err:=api.UpdateBatch(operate_table,updateWhere,actionFiledMap)
				if err!=nil{
					fmt.Printf("err=",err)
				}
				fmt.Printf("rsU=",rsU)

			}
			//	}
			if action_type=="UPDATE_STATUS"{
				//updateWhere:=make(map[string]WhereOperation)
				//if option.ExtendedMap[conditionFieldKey]!=nil{
				//	updateWhere[conditionFieldKey]=WhereOperation{
				//		Operation:"eq",
				//		Value:option.ExtendedMap[conditionFieldKey].(string),
				//	}
				//
				//}
				if option.ExtendedMap[conditionFieldKey]!=nil{
					updateSql:="select "+operateFunc+"('"+option.ExtendedMap[conditionFieldKey].(string)+"')"
					result,errorMessage:=api.ExecFunc(updateSql)
					fmt.Printf("result=",result,"errorMessage=",errorMessage)
				}

				//rsU,err:=api.UpdateBatch(operate_table,updateWhere,actionFiledMap)
				//if err!=nil{
				//	fmt.Printf("err=",err)
				//}
				//fmt.Printf("rsU=",rsU)

			}
			//	}


			if pri_key_value!=""{
				rsU,err:=	api.Update(operate_table,pri_key_value,actionFiledMap)
				if err!=nil{
					fmt.Print("err=",err)
				}

				rowesAffected,error:=rsU.RowsAffected()
				if error!=nil{
					fmt.Printf("err=",error)
				}else{
					fmt.Printf("rowesAffected=",rowesAffected)
				}

			}

		}
		if "OBTAIN_FROM_LOCAL" == conditionType {
			for _, item := range conditionFiledArr {
				if item != "" {
					fieldList.PushBack(item)
				}
			}
			//  从参数里获取配置中字段的值
			var count int64
			for e := fieldList.Front(); e != nil; e = e.Next() {

				for _,itemMap:=range option.ExtendedArr{
					if itemMap[e.Value.(string)]!=nil{
						fielVale := itemMap[e.Value.(string)].(string)

						// 操作类型级联删除
						if operate_type == "CASCADE_DELETE" && fielVale != "" {

							api.Delete(operate_table, fielVale, nil)
							count=count+1


						}
					}

				}


			}
			//	return c.String(http.StatusOK, strconv.FormatInt(rowesAffected, 10))
		}
         // CAL_CREDIT_SCORE_LEVEL
        if "CAL_CREDIT_SCORE_LEVEL"==operate_type{
        	// 如果请求方式是DELETE 构造option.ExtendedMap对象只有filterFieldKey
        	if equestMethod=="DELETE" && option.ExtendedMap!=nil{
				 extendDelMap:=make(map[string]interface{})
				extendDelMap[conditionFieldKey]=conditionFieldKeyValue
				option.ExtendedMap=extendDelMap

			}
			// 根据每一行构建查询条件
			whereOption0 := map[string]WhereOperation{}
			whereOption0[conditionFieldKey]=WhereOperation{
				Operation:"eq",
				Value:option.ExtendedMap[conditionFieldKey],
			}// rating_status
			whereOption0["rating_status"]=WhereOperation{
				Operation:"neq",
				Value:"2",
			}

			querOption0 := QueryOption{Wheres: whereOption0, Table: "customer_related_view"}
			rsQuery0, errorMessage:= api.Select(querOption0)
			var farm_type string
			for _,item:=range rsQuery0{
				fmt.Printf("item=",item)
				farm_type=item["farm_type"].(string)

				break
			}


			// customer_type
			option.Table=conditionTable
			// 根据每一行构建查询条件
			whereOption := map[string]WhereOperation{}
			whereOption["farm_type"]=WhereOperation{
				Operation:"eq",
				Value:farm_type,
			}
			whereOption["first_level_norm"]=WhereOperation{
				Operation:"like",
				Value:"%"+tableName+"%",
			}
			whereOption["is_enable"]=WhereOperation{
				Operation:"eq",
				Value:"1",
			}
			querOption := QueryOption{Wheres: whereOption, Table: conditionTable}
			rsQuery, errorMessage:= api.Select(querOption)
			if errorMessage!=nil{
				fmt.Printf("errorMessage", errorMessage)
			}else{
				fmt.Printf("rs", rsQuery)
			}
			for _,item:=range rsQuery{
				// 先拦截器校验
				if filterFunc!=""{

					filterFuncSql:="select "+filterFunc+"('"+ConverStrFromMap(filterFieldKey,option.ExtendedMap)+"') as result;"
					filterResult:=api.ExecFuncForOne(filterFuncSql,"result")
					if filterResult==""{
						break
					}
				}
				var extraOperateMap map[string]interface{}
				extra:=item["extra"].(string)// {"pre_operate_func":"obtainAge","judge_type":"between","operate_type":"eq","operate_func":"calScore"}
				if(extra!=""){
					json.Unmarshal([]byte(extra), &extraOperateMap)
				}
				var extraOperateType string
				if extraOperateMap["operate_type"]!=nil{
					extraOperateType=extraOperateMap["operate_type"].(string)
				}
				if (extraOperateType=="add" && equestMethod=="PATCH") || (extraOperateType=="add" && equestMethod=="DELETE") {
					//  通过关联id查询出所有  重新计算
					whereOptionPatch := map[string]WhereOperation{}
					whereOptionPatch[conditionFieldKey]=WhereOperation{
						Operation:"eq",
						Value:option.ExtendedMap[conditionFieldKey],
					}

					querOptionPatch := QueryOption{Wheres: whereOptionPatch, Table: tableName}
					rsQueryPatch, errorMessage:= api.Select(querOptionPatch)
					fmt.Printf("errorMessage=",errorMessage)
					// 先删掉 已经计算出来的得分记录  然后重新计算
					deleteWhere:=make(map[string]interface{})
					deleteWhere[conditionFieldKey]=conditionFieldKeyValue
					deleteWhere["credit_level_model_id"]=item["credit_level_model_id"]
					_,errorMessage=api.Delete("credit_level_customer",nil,deleteWhere)
					fmt.Printf("errorMessage=",errorMessage)
					for _,rsQueryPatchItem:=range rsQueryPatch{
						CallLevel(api,item,extraOperateMap,rsQueryPatchItem,conditionFieldKeyValue)
					}

				}else{

					CallLevel(api,item,extraOperateMap,option.ExtendedMap,conditionFieldKeyValue)

				}



			}

		}
		if "CAL_DEPEND_FIELD"==operate_type {
			if operateFunc!=""{
				operateFuncSql:="select "+operateFunc+"('"+conditionFieldKeyValue+"') as result;"
				result:=api.ExecFuncForOne(operateFuncSql,"result")
				if result!=""{
					option.ExtendedMap[conditionFieldKey]=result
				}
				fmt.Printf("errorMessage=",errorMessage)

			}
		}
		if "SYNC"==operate_type {
			if operateFunc!=""{
				operateFuncSql:="select "+operateFunc+"('"+conditionFieldKeyValue+"') as result;"
				result:=api.ExecFuncForOne(operateFuncSql,"result")
				fmt.Printf("result=",result)
				fmt.Printf("errorMessage=",errorMessage)

			}
		}
		if "SYNC_COMPLEX"==operate_type{
			if operateFunc!=""{
				for _,item:=range option.ExtendedArr{
					operateFuncSql:="select "+operateFunc+"('"+item[conditionFieldKey].(string)+"') as result;"
					result:=api.ExecFuncForOne(operateFuncSql,"result")
					fmt.Printf("result=",result)

				}

			}
		}
// limit
	}

	// {"conditionType":"JUDGE","conditionTable":"customer.shopping_cart","conditionFields":"[\"customer_id\",\"goods_id\"]"}

	return data,nil;
}
func CallLevel(api adapter.IDatabaseAPI,item map[string]interface{},extraOperateMap map[string]interface{},extendMap map[string]interface{},conditionFieldKeyValue string){
	fmt.Printf("item=",item)
	credit_level_model_id:=item["credit_level_model_id"].(string)
	var pre_operate_func string

	var operate_func string


	if extraOperateMap["pre_operate_func"]!=nil{
		pre_operate_func=extraOperateMap["pre_operate_func"].(string)
	}

	if extraOperateMap["operate_func"]!=nil{
		operate_func=extraOperateMap["operate_func"].(string)
	}
	// second_level_norm
	second_level_norm:=item["second_level_norm"].(string)
	second_level_norm_arr:=strings.Split(second_level_norm,"-")
	// 如果二级指标有值 则进行处理
	var result string

	if len(second_level_norm_arr)==1{
		if extendMap[second_level_norm_arr[0]]!=nil{
			// 如果二级指标只有一个字段参与计算

			if pre_operate_func!=""{
				pre_operate_func_sql:="select "+pre_operate_func+"('"+ConverStrFromMap(second_level_norm_arr[0],extendMap)+"','"+conditionFieldKeyValue+"') as result;"
				result=api.ExecFuncForOne(pre_operate_func_sql,"result")
			}
			if result==""{
				result=ConverStrFromMap(second_level_norm_arr[0],extendMap)
			}

		}
	}else if len(second_level_norm_arr)>1{
		// 如果二级指标有多个字段参与计算
		//fmt.Printf("judge_type=",judge_type," judge_content=",judge_content)
		var paramStr string
		for index,item:=range second_level_norm_arr{
			if index==(len(second_level_norm_arr)-1){
				paramStr=paramStr+"'"+ConverStrFromMap(item,extendMap)+"'"
			}else{
				paramStr=paramStr+"'"+ConverStrFromMap(item,extendMap)+"',"
			}

		}
		if pre_operate_func!=""{
			pre_operate_func_sql:="select "+pre_operate_func+"("+paramStr+",'"+conditionFieldKeyValue+"') as result;"
			result=api.ExecFuncForOne(pre_operate_func_sql,"result")
		}

	}

	//fmt.Printf("errorMessage=",errorMessage)
	operate_func_sql:="select "+operate_func+"('"+result+"','"+conditionFieldKeyValue+"','"+credit_level_model_id+"') as result;"
	result1:=api.ExecFuncForOne(operate_func_sql,"result")
	fmt.Printf("result1=",result1)

}
func CallFunc(api adapter.IDatabaseAPI,calculate_field string,calculate_func string,paramStr string,asyncObjectMap map[string]interface{})(map[string]interface{}){
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

func CalculatePre(api adapter.IDatabaseAPI,repeatItem map[string]interface{},funcParamFields []string,pre_subject_key string,operate_func string){

	//	asyncObjectMap=BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
	//asyncObjectMap=BuildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)
	//slave["subject_key_pre"]=slave["subject_key"]
	////fmt.Printf("operate_table",operate_table)
	//fmt.Printf("calculate_field",calculate_field)
	//fmt.Printf("calculate_func",calculate_func)

	paramsMap:=make(map[string]interface{})
	// funcParamFields
	if operate_func!=""{

		//如果执行方法不为空 执行配置中方法
		paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
		in_subject_key:=paramsMap["subject_key"].(string)
		if pre_subject_key==in_subject_key{
			return
		}

		//	in_farm_id:=paramsMap["farm_id"].(string)
		//obtianPreSubjectSql:="select obtainPreSubjectKey('"+in_subject_key+"','"+in_farm_id+"'"+") as pre_subject_key;"
		//pre_subject_key:=api.ExecFuncForOne(obtianPreSubjectSql,"pre_subject_key")
		paramsMap["subject_key_pre"]=pre_subject_key
		//把对象的所有属性的值拼成字符串
		paramsMap["id"]=repeatItem["id"]
		paramsMap["account_period_year"]=repeatItem["account_period_year"]
		paramStr:=ConcatObjectProperties(funcParamFields,paramsMap)

		if pre_subject_key!="" && pre_subject_key!=in_subject_key{
			// 直接执行func 所有逻辑在func处理
			operate_func_sql:="select "+operate_func+"("+paramStr+") as result;"
			result:=api.ExecFuncForOne(operate_func_sql,"result")
			fmt.Printf("operate_func_sql-result",result)
		}




	}

}
