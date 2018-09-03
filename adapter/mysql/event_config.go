package mysql

import (
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
	//	var actionType string
	var conditionFiledArr [5]string
	var resultFieldsArr [5]string
	//var actionFieldsArr [5]string
	var operateCondJsonMap map[string]interface{}
	var operateCondContentJsonMap map[string]interface{}
	fieldList:=list.New()
	for _,operate:=range operates {
		operate_condition= operate["operate_condition"].(string)
		operate_content = operate["operate_content"].(string)

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
				fieldList.PushBack(item)
			}
		}

		var conditionFieldKey string
		if operateCondJsonMap["conditionFieldKey"]!=nil{
			conditionFieldKey=operateCondJsonMap["conditionFieldKey"].(string)
		}
		if operateCondContentJsonMap["operate_func"]!=nil{
			operateFunc=operateCondContentJsonMap["operate_func"].(string)
		}

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
	}

	// {"conditionType":"JUDGE","conditionTable":"customer.shopping_cart","conditionFields":"[\"customer_id\",\"goods_id\"]"}

	return data,nil;
}


//后置事件处理
func PostEvent(api adapter.IDatabaseAPI,tableName string ,equestMethod string,data []map[string]interface{},option QueryOption,redisHost string)(rs []map[string]interface{},errorMessage *ErrorMessage){
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
	//	var actionType string
	var conditionFiledArr [5]string
	var resultFieldsArr [5]string
	var actionFieldsArr [5]string
	var operateCondJsonMap map[string]interface{}
	var operateCondContentJsonMap map[string]interface{}
	fieldList:=list.New()
	for _,operate:=range operates {
		operate_condition= operate["operate_condition"].(string)
		operate_content = operate["operate_content"].(string)

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
		for _,item:= range conditionFiledArr{
			if item!=""{
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

		//判断条件类型 如果是JUDGE 判断是否存在 如果存在做操作后动作
		// {"operate_type":"UPDATE","pri_key":"id","action_type":"ACC","action_field":"goods_num"}
		operate_type=operateCondContentJsonMap["operate_type"].(string)
		operate_table=operateCondContentJsonMap["operate_table"].(string)
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
			switch  option.ExtendedArr[0][conditionFieldKey].(type) {
			case string:
				conditionFieldKeyValueStr=option.ExtendedArr[0][conditionFieldKey].(string)
			case 	float64:
				conditionFieldKeyValueStr=strconv.FormatFloat(option.ExtendedArr[0][conditionFieldKey].(float64), 'f', -1, 64)
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
			if action_type=="UPDATE_ACCOUNT_RECORD"{
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

				rsU,err:=api.UpdateBatch(operate_table,updateWhere,actionFiledMap)
				if err!=nil{
					fmt.Printf("err=",err)
				}
				fmt.Printf("rsU=",rsU)

			}
			//	}
			if action_type=="UPDATE_STATUS" && len(rsQuery)==0{
				updateWhere:=make(map[string]WhereOperation)
				if option.ExtendedMap[conditionFieldKey]!=nil{
					updateWhere[conditionFieldKey]=WhereOperation{
						Operation:"eq",
						Value:option.ExtendedMap[conditionFieldKey].(string),
					}

				}

				rsU,err:=api.UpdateBatch(operate_table,updateWhere,actionFiledMap)
				if err!=nil{
					fmt.Printf("err=",err)
				}
				fmt.Printf("rsU=",rsU)

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

	}

	// {"conditionType":"JUDGE","conditionTable":"customer.shopping_cart","conditionFields":"[\"customer_id\",\"goods_id\"]"}

	return data,nil;
}

