package mysql

import (
	"bytes"
	"container/list"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/shiyongabc/go-sql-api/adapter"
	"github.com/shiyongabc/go-sql-api/server/lib"
	"github.com/shiyongabc/go-sql-api/server/util"
	. "github.com/shiyongabc/go-sql-api/types"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 异步任务
func AsyncEvent(api adapter.IDatabaseAPI,tableName string ,equestMethod string,data []map[string]interface{},option QueryOption,redisHost string)(rs []map[string]interface{},errorMessage *ErrorMessage){
	//tx,err:=api.Connection().Begin()
	//fmt.Print("//tx-error",err)
	operates,errorMessage:=	SelectOperaInfo(api,api.GetDatabaseMetadata().DatabaseName+"."+tableName,equestMethod,"1")
	if len(operates)==0{
		return
	}

	lib.Logger.Error("errorMessage=%s",errorMessage)

	for _,operate:=range operates {
		var operateCondContentJsonMap map[string]interface{}
		var operateCondJsonMap map[string]interface{}

		var operateFilterContentJsonMap map[string]interface{}
		var operateFunc string
		var operateFunc1 string
		var operateProcedure string
		var resultFieldsArr []string
		var conditionFiledArr []string
		var conditionFiledArr1 []string
		var operateScipt string
		var operate_condition string
		var operate_content string
		var filter_content string
		var conditionType string
		var conditionTable string
		var conditionFileds string
		var conditionFileds1 string
		var resultFileds string
		var operate_type string
		var operate_table string

		var filterFunc string
		var filterFieldKey string
		//	var actionType string
		var actionType string
		var filterFiledArr []string
		var filterFiledArrStr string

		fieldList:=list.New()

		operate_condition= operate["operate_condition"].(string)
		operate_content = operate["operate_content"].(string)
		filter_content = operate["filter_content"].(string)

		if(operate_condition!=""){
			operate_condition=strings.Replace(operate_condition,"\r\n","",-1)
			json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)
			if operateCondJsonMap["conditionType"]!=nil{
				conditionType=operateCondJsonMap["conditionType"].(string)
				fmt.Print(conditionType)
			}

			if operateCondJsonMap["conditionFields"]!=nil{
				conditionFileds=operateCondJsonMap["conditionFields"].(string)
			}
			if operateCondJsonMap["conditionFields1"]!=nil{
				conditionFileds1=operateCondJsonMap["conditionFields1"].(string)
			}
			if operateCondJsonMap["resultFields"]!=nil{
				resultFileds=operateCondJsonMap["resultFields"].(string)
			}
			if operateCondJsonMap["conditionTable"]!=nil{
				conditionTable=operateCondJsonMap["conditionTable"].(string)
			}

			json.Unmarshal([]byte(conditionFileds), &conditionFiledArr)
			json.Unmarshal([]byte(conditionFileds1), &conditionFiledArr1)
			json.Unmarshal([]byte(resultFileds), &resultFieldsArr)
		}
		if(operate_content!=""){
			operate_content=strings.Replace(operate_content,"\r\n","",-1)

			json.Unmarshal([]byte(operate_content), &operateCondContentJsonMap)
		}
		if(filter_content!=""){
			filter_content=strings.Replace(filter_content,"\r\n","",-1)
			json.Unmarshal([]byte(filter_content), &operateFilterContentJsonMap)
		}
		if operateFilterContentJsonMap["filterFunc"]!=nil{
			filterFunc=operateFilterContentJsonMap["filterFunc"].(string)
		}
		if operateFilterContentJsonMap["filterFieldKey"]!=nil{
			filterFieldKey=operateFilterContentJsonMap["filterFieldKey"].(string)
		}
		if operateFilterContentJsonMap["filterFields"]!=nil{
			filterFiledArrStr=operateFilterContentJsonMap["filterFields"].(string)
			json.Unmarshal([]byte(filterFiledArrStr), &filterFiledArr)
		}
		var isFiltered bool
		if strings.Contains(filterFieldKey,"="){
			arr:=strings.Split(filterFieldKey,"=")
			field0:=arr[0]
			value0:=arr[1]
			if option.ExtendedMap[field0]==value0{
				isFiltered=true
				break;
			}else{
				isFiltered=false
			}
		}else if filterFieldKey==""{
			isFiltered=true
		}
		// 如果不满足过滤条件 则不执行当前事件
		if !isFiltered{
			continue
		}

		if filterFunc!=""{
			filterFuncSql:="select "+filterFunc+"('"+ConverStrFromMap(filterFieldKey,option.ExtendedMap)+"') as result;"
			filterResult,errorMessage:=api.ExecFuncForOne(filterFuncSql,"result")
			if errorMessage!=nil{
				//tx.Rollback()
			}
			if filterResult!=""{
				continue
			}
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
		// actionType
		if operateCondContentJsonMap["action_type"]!=nil{
			actionType=operateCondContentJsonMap["action_type"].(string)
		}
		// operateScipt
		if operateCondContentJsonMap["operate_script"]!=nil{
			operateScipt=operateCondContentJsonMap["operate_script"].(string)
			lib.Logger.Infof("operateScipt=",operateScipt)
		}
		if operateCondContentJsonMap["operate_func"]!=nil{
			operateFunc=operateCondContentJsonMap["operate_func"].(string)
			lib.Logger.Infof("operateFunc=",operateFunc)
		}
		if operateCondContentJsonMap["operate_func1"]!=nil{
			operateFunc1=operateCondContentJsonMap["operate_func1"].(string)
			lib.Logger.Infof("operate_func1=",operateFunc1)
		}
		// operate_table
		if operateCondContentJsonMap["operate_table"]!=nil{
			operate_table=operateCondContentJsonMap["operate_table"].(string)
			lib.Logger.Infof("operate_table=",operate_table)
		}
		// operateProcedure
		if operateCondContentJsonMap["operate_procedure"]!=nil{
			operateProcedure=operateCondContentJsonMap["operate_procedure"].(string)
			lib.Logger.Infof("operateProcedure=",operateProcedure)
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
				lib.Logger.Infof("item=",item)
				farm_type_o:=item["farm_type"]

				if farm_type_o==nil {
					farm_type="1"
				}else{
					farm_type=farm_type_o.(string)
				}

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
				lib.Logger.Error("errorMessage=%s", errorMessage)
			}else{
				lib.Logger.Infof("rs", rsQuery)
			}
			for _,item:=range rsQuery{
				// 先拦截器校验
				if filterFunc!=""{

					filterFuncSql:="select "+filterFunc+"('"+ConverStrFromMap(filterFieldKey,option.ExtendedMap)+"') as result;"
					filterResult,errorMessage:=api.ExecFuncForOne(filterFuncSql,"result")
					fmt.Print(errorMessage)

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
					lib.Logger.Error("errorMessage=%s",errorMessage)
					// 先删掉 已经计算出来的得分记录  然后重新计算
					deleteWhere:=make(map[string]interface{})
					deleteWhere[conditionFieldKey]=conditionFieldKeyValue
					deleteWhere["credit_level_model_id"]=item["credit_level_model_id"]
					_,errorMessage=api.Delete("credit_level_customer",nil,deleteWhere)
					lib.Logger.Error("errorMessage=%s",errorMessage)
					for _,rsQueryPatchItem:=range rsQueryPatch{
						CallLevel(api,item,extraOperateMap,rsQueryPatchItem,conditionFieldKeyValue)
					}

				}else{

					CallLevel(api,item,extraOperateMap,option.ExtendedMap,conditionFieldKeyValue)

				}



			}

		}
		if "SYNC"==operate_type {
			if operateFunc!=""{
				if conditionFieldKeyValue!=""{
					operateFuncSql:="select "+operateFunc+"('"+conditionFieldKeyValue+"') as result;"
					result,errorMessage:=api.ExecFuncForOne(operateFuncSql,"result")
					lib.Logger.Infof("result=",result)
					lib.Logger.Error("errorMessage=%s",errorMessage)
					if errorMessage!=nil{
						//tx.Rollback()
					}
				}else if len(conditionFiledArr)>0{
					paramsFunc:=ConcatObjectProperties(conditionFiledArr,option.ExtendedMap)
					paramArr:=strings.Split(paramsFunc,",")
					if len(conditionFiledArr)!=len(paramArr){
						lib.Logger.Error("errorMessage=%s","function "+operateFunc+" params count is not match input")
						continue
					}
					if paramsFunc!=""{
						operateFuncSql:="select "+operateFunc+"("+paramsFunc+");"
						result,errorMessage:=api.ExecFuncForOne(operateFuncSql,"result")
						lib.Logger.Infof("result=",result)
						lib.Logger.Error("errorMessage=%s",errorMessage)
						errorMessage=errorMessage

					}
				}


			}
			if operateProcedure!=""{
				if conditionFieldKeyValue!=""{
					operateProcedureSql:="CALL "+operateProcedure+"('"+conditionFieldKeyValue+"');"
					result,errorMessage:=api.ExecFuncForOne(operateProcedureSql,"result")
					lib.Logger.Infof("result=",result)
					lib.Logger.Error("errorMessage=%s",errorMessage)
					if errorMessage!=nil {
						//tx.Rollback()
					}
				}else if len(conditionFiledArr)>0{
					paramsPro:=ConcatObjectProperties(conditionFiledArr,option.ExtendedMap)
					if paramsPro!=""{
						operateProcedureSql:="CALL "+operateProcedure+"("+paramsPro+");"
						result,errorMessage:=api.ExecFuncForOne(operateProcedureSql,"result")
						lib.Logger.Infof("result=",result)
						lib.Logger.Error("errorMessage=%s",errorMessage)
						if errorMessage!=nil{
							//tx.Rollback()
						}
					}
				}

			}
		}
		if "SYNC_COMPLEX"==operate_type{
			if operateFunc!=""{
				var syncComplexOption QueryOption
				syncComplexOption.Table=tableName
				complexWhereMap:= map[string]WhereOperation{}
				complexWhereMap[tableName+"."+conditionFieldKey]=WhereOperation{
					"eq",
					option.ExtendedMap[conditionFieldKey],
				}

				syncComplexOption.Wheres=complexWhereMap
				syncComplexData, errorMessage:= api.Select(syncComplexOption)
				fmt.Print("syncComplexData=",syncComplexData)
				fmt.Print(errorMessage)
				for _,item:=range syncComplexData{
					operateFuncSql:="select "+operateFunc+"('"+item["id"].(string)+"') as result;"
					result,errorMessage:=api.ExecFuncForOne(operateFuncSql,"result")
					lib.Logger.Infof("result=",result)
				    if errorMessage!=nil{
				    	//tx.Rollback()
					}
				}

			}
		}
		if "EMBED_SCRIPT"==operate_type {
			// actionType SINGLE_PROCESS   MUTIL_PROCESS   MUTIL_PROCESS_FOR
			// SINGLE_PROCESS 单条sql处理
			// MUTIL_PROCESS  多条sql处理 无变量依赖
			// MUTIL_PROCESS_COMPLEX  多条sql处理  有变量依赖

			if operateScipt==""  {
				continue
			}
			if actionType=="SINGLE_PROCESS"{
				result,errorMessage:=SingleExec(api,option,conditionFiledArr,operateScipt)
				if result!="" && conditionFieldKey!="" && errorMessage.ErrorDescription!=""{
					option.ExtendedMap[conditionFieldKey]=result
				}
			}
			if actionType=="MUTIL_PROCESS"{
				operateSciptArr:=strings.Split(operateScipt,";")
				for _,itemScript:=range operateSciptArr{
					result,errorMessage:=SingleExec(api,option,conditionFiledArr,itemScript)
					lib.Logger.Infof("result=",result,"errorMessage=",errorMessage)
				}
			}
			if actionType=="MUTIL_PROCESS_COMPLEX"{
				// 存放script的变量和计算出的值
				varMap:=make(map[string]interface{})
				operateSciptArr :=strings.Split(operateScipt,"!!")
				for _,itemScript:=range operateSciptArr{
					// 解析itemScript
					// 解析模块类型 是赋值还还是返回值类型
					// 判断类型 /*JUDGE*/  for循坏类型 /*FOR_HANDLE*/  while循坏类型 /*WHILE_HANDLE*/  复杂逻辑在一个function里处理(这个方法里没有包含分表的表操作)
					// 如果有判断类型(判断的同时会有赋值类型和同步类型和返回类型)  这里默认判断nil  单个判断/*JUDGE_SINGLE*/  多个判断且/*JUDGE_AND*/  多个判断或/*JUDGE_OR*/
					if strings.Contains(itemScript,"/*JUDGE_SINGLE*/"){
						// /*ASS_VAR*//*JUDE_SINGLE*/$Stest$E SET maxNo=(SELECT MAX(`stu_no`) AS result FROM test.`stu`);
						varParam:=util.GetBetweenStr(itemScript,"$S","$E")
						if varMap[varParam]!=nil && varMap[varParam]!=""{
							itemScript=strings.Replace(itemScript,"/*JUDGE_SINGLE*/$S"+varParam+"$E","",-1)
						}else{
							continue
						}
					}
					if strings.Contains(itemScript,"/*JUDGE_AND*/"){
						// /*ASS_VAR*//*JUDGE_AND*/$Stest-tt$E SET maxNo=(SELECT MAX(`stu_no`) AS result FROM test.`stu`);
						varParamStr:=util.GetBetweenStr(itemScript,"$S","$E")
						paramArr:=strings.Split(varParamStr,"-")
						var param0 string
						var param1 string
						if len(paramArr)>0{
							param0=paramArr[0]
							param1=paramArr[1]
						}
						if varMap[param0]!=nil && varMap[param0]!="" && varMap[param1]!=nil && varMap[param1]!=""{
							itemScript=strings.Replace(itemScript,"/*JUDGE_AND*/$S"+varParamStr+"$E","",-1)
						}else{
							continue
						}
					}
					if strings.Contains(itemScript,"/*JUDGE_OR*/"){
						// /*ASS_VAR*//*JUDGE_OR*/$Stest-tt$E SET maxNo=(SELECT MAX(`stu_no`) AS result FROM test.`stu`);
						varParamStr:=util.GetBetweenStr(itemScript,"$S","$E")
						paramArr:=strings.Split(varParamStr,"-")
						var param0 string
						var param1 string
						if len(paramArr)>0{
							param0=paramArr[0]
							param1=paramArr[1]
						}
						if (varMap[param0]!=nil && varMap[param0]!="") || (varMap[param1]!=nil && varMap[param1]!=""){
							itemScript=strings.Replace(itemScript,"/*JUDGE_OR*/$S"+varParamStr+"$E","",-1)
						}else{
							continue
						}
					}

					if strings.Contains(itemScript,"/*ASS_VAR*/"){
						itemScript=strings.Replace(itemScript,"/*ASS_VAR*/","",-1)
						// 赋值类型： 一个变量赋值  多个变量赋值


						assVarArr:=strings.Split(itemScript,"INTO")
						if len(assVarArr)>1 {
							// INTO 方式赋值
							// SELECT stu_no,project_name into stuNo,projectName from test.stu_score  替换为
							// SELECT stu_no as stuNo, project_name as projectName from test.stu_score
							var execSql  strings.Builder
							execSql.WriteString("SELECT ")
							intoStr:=assVarArr[1]
							assVarStr:=strings.Replace(assVarArr[0],"SELECT","",-1)
							assVarStr=strings.Trim(assVarStr," ")
							fieldArr:=strings.Split(assVarStr,",")
							//取into字段

							fromIndex:=strings.Index(intoStr,"FROM")
							var endStr string
							if fromIndex>0{
								endStr=intoStr[fromIndex:]
								intoStr=intoStr[:fromIndex]
							}

							intoArr:=strings.Split(intoStr,",")

							for i,item:=range intoArr{
								// 去掉前后空格
								varItemTrim:=strings.Trim(fieldArr[i]," ")
								intoItemTrim:=strings.Trim(item," ")
								execSql.WriteString(varItemTrim)
								execSql.WriteString(" as ")
								execSql.WriteString(intoItemTrim)
								if i<(len(intoArr)-1){
									execSql.WriteString(",")
								}

							}
							execSql.WriteString(" "+endStr)
							//
							result,errorMessage:=MutilExec(api,option,conditionFiledArr,varMap,execSql.String())
							lib.Logger.Error("errorMessage=%s",errorMessage)
							for _,item:=range intoArr{
								key:=strings.Trim(item," ")
								var value string
								if len(result)>0{
									value=InterToStr(result[0][key])
								}else{
									value=""
								}

								varMap[key]=value
							}
						}
						if len(assVarArr)==1 {
							// INTO 方式赋值
							// SELECT stu_no,project_name into stuNo,projectName from test.stu_score  替换为
							// SELECT stu_no as stuNo, project_name as projectName from test.stu_score
							var execSql  strings.Builder
							assVarStr:=strings.Replace(assVarArr[0],"SELECT","",-1)
							fromIndex:=strings.Index(assVarStr,"FROM")
							assVarStr=assVarStr[0:fromIndex]
							assVarStr=strings.Trim(assVarStr," ")
							fieldArr:=strings.Split(assVarStr,",")
							//取into字段

							execSql.WriteString(itemScript)
							//
							result,errorMessage:=MutilExec(api,option,conditionFiledArr,varMap,execSql.String())
							lib.Logger.Error("errorMessage=%s",errorMessage)
							for _,item:=range fieldArr{
								key:=strings.Trim(item," ")
								var value string
								if len(result)>0{
									value=InterToStr(result[0][key])
								}else{
									value=""
								}

								varMap[key]=value
							}
						}
					}
					if strings.Contains(itemScript,"/*ASS_VAR_FROM_FUNCS*/"){
						itemScript=strings.Replace(itemScript,"/*ASS_VAR_FROM_FUNCS*/","",-1)
						// 赋值类型： 一个变量赋值  多个变量赋值

						assVarArr:=strings.Split(itemScript,"INTO")
						if len(assVarArr)>1 {
							// INTO 方式赋值
							// SELECT stu_no,project_name into stuNo,projectName from test.stu_score  替换为
							// SELECT stu_no as stuNo, project_name as projectName from test.stu_score
							var execSql  strings.Builder
							execSql.WriteString(strings.Replace(itemScript,"INTO","AS",-1))
							intoStr:=assVarArr[1]
							intoStr=strings.Replace(intoStr,";","",-1)
							intoStr=strings.Trim(intoStr," ")

							//
							result,errorMessage:=MutilExec(api,option,conditionFiledArr,varMap,execSql.String())
							lib.Logger.Error("errorMessage=%s",errorMessage)
							var value string
							if len(result)>0{
								value=InterToStr(result[0][intoStr])
							}else{
								value=""
							}
							varMap[intoStr]=value
						}
					}

					//  同步类型/*SYNC_HANDLE*/
					if strings.Contains(itemScript,"/*SYNC_HANDLE*/"){
						itemScript=strings.Replace(itemScript,"/*SYNC_HANDLE*/","",-1)
						result,errorMessage:=SingleExec1(api,option,conditionFiledArr,varMap,itemScript)
						if errorMessage!=nil{
							lib.Logger.Infof("sync_handle-result=",result," errorMessage=",errorMessage)
						}

					}
					//  返回类型  /*RETURN_HANDLE*/
					if strings.Contains(itemScript,"/*RETURN_HANDLE*/"){
						varParam:=strings.Replace(itemScript,"RETURN","",-1)
						varParam=strings.Replace(varParam,"(","",-1)
						varParam=strings.Replace(varParam,");","",-1)
						if varMap[varParam]!=nil && varMap[varParam]!=""{
							option.ExtendedMap[conditionFieldKey]=varMap[varParam]
						}
					}





				}
			}



		}
		if "PUSH_MES"==operate_type{
			client := &http.Client{}
			var title string
			var content string
			var userIds string
			var msgType int
			var BusMsgType int
			var MsgKey string
			var err error
			if operateCondContentJsonMap["msg_type"]!=nil{
				msgType, err = strconv.Atoi(operateCondContentJsonMap["msg_type"].(string))
				lib.Logger.Infof("msgType=",msgType)
				if err!=nil{
					lib.Logger.Infof("err=",err.Error())
				}
			}
			if operateCondContentJsonMap["bus_msg_type"]!=nil{
				BusMsgType, err = strconv.Atoi(operateCondContentJsonMap["bus_msg_type"].(string))
				lib.Logger.Infof("bus_msg_type=",BusMsgType)
				if err!=nil{
					lib.Logger.Infof("err=",err.Error())
				}
			}
			// MsgKey
			if operateCondContentJsonMap["msg_key"]!=nil{
				MsgKey= operateCondContentJsonMap["msg_key"].(string)
				lib.Logger.Infof("msg_key=",MsgKey)
				if err!=nil{
					lib.Logger.Infof("err=",err.Error())
				}
			}
			if operateCondContentJsonMap["title"]!=nil{
				title=operateCondContentJsonMap["title"].(string)
				lib.Logger.Infof("title=",title)
			}
			// 构造请求参数
			userIdsParam:=ConcatObjectProperties(conditionFiledArr,option.ExtendedMap)
			contentParam:=ConcatObjectProperties(conditionFiledArr1,option.ExtendedMap)
			if operateFunc!=""&&userIdsParam!=""{
				userIdsFuncSql:="select "+operateFunc+"("+userIdsParam+") as result;"
				userIds,errorMessage=api.ExecFuncForOne(userIdsFuncSql,"result")
				lib.Logger.Infof("userIds=",userIds)
				lib.Logger.Error("errorMessage=%s",errorMessage)
				errorMessage=errorMessage

			}
			if operateFunc1!=""&&contentParam!=""{
				conctentFuncSql:="select "+operateFunc1+"("+contentParam+") as result;"
				content,errorMessage=api.ExecFuncForOne(conctentFuncSql,"result")
				lib.Logger.Infof("content=",content)
				lib.Logger.Error("errorMessage=%s",errorMessage)
				errorMessage=errorMessage

			}
			if content==""{
				continue
			}
			pushParamMap:=make(map[string]interface{})
			pushParamMap["senderId"]=1
			pushParamMap["senderName"]="SYSTEM"
			pushParamMap["userIds"]=userIds
			pushParamMap["msgType"]=msgType
			pushParamMap["busMsgType"]=BusMsgType
			pushParamMap["msgKey"]=MsgKey
			pushParamMap["title"]=title
			pushParamMap["content"]=content
			pushParamMap["timestamp"]=time.Now().Format("2006-01-02 15:04:05")
			pushParamMapBytes,err:=json.Marshal(pushParamMap)
			fmt.Print("err",err)
			reqest, err := http.NewRequest("POST", operate_table, bytes.NewBuffer(pushParamMapBytes))
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

			}else{
				// errorMessage.ErrorDescription="api remote error"
			}

		}
		// limit
	}

	// {"conditionType":"JUDGE","conditionTable":"customer.shopping_cart","conditionFields":"[\"customer_id\",\"goods_id\"]"}

	return data,nil;
}
// 异步执行多个option  AsyncEventArr
func AsyncEventArr(api adapter.IDatabaseAPI,tableName string ,equestMethod string,data []map[string]interface{},option QueryOption,redisHost string)(rs []map[string]interface{},errorMessage *ErrorMessage){
    for _,item:=range option.ExtendedArr{
    	var option QueryOption
    	option.ExtendedMap=item
		AsyncEvent(api,tableName ,equestMethod,data,option,redisHost)
	}
	return data,nil;
}


//前置事件处理

func PreEvent(api adapter.IDatabaseAPI,tableName string ,equestMethod string,data []map[string]interface{},option QueryOption,redisHost string)(rs []map[string]interface{},errorMessage *ErrorMessage){
	//tx,err:=api.Connection().Begin()
	//fmt.Print("//tx-error",err)
	operates,errorMessage:=	SelectOperaInfo(api,api.GetDatabaseMetadata().DatabaseName+"."+tableName,equestMethod,"0")
	lib.Logger.Error("errorMessage=%s",errorMessage)


	for _,operate:=range operates {
		var fields []string
		var conditionFiledArr []string
		var resultFieldsArr []string
		var filterFiledArr []string
		var actionFiledArr []string
		var operateCondJsonMap map[string]interface{}
		var operateCondContentJsonMap map[string]interface{}
		var filterCondContentJsonMap map[string]interface{}
		var operateScipt string
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

		var actionFiledArrStr string

		var filter_content string

		var filterFiledArrStr string
		var filterKey string
		var filterFunc string
		//	var actionType string

		//var actionFieldsArr [5]string

		fieldList:=list.New()
		operate_condition= operate["operate_condition"].(string)
		operate_content = operate["operate_content"].(string)
		filter_content=operate["filter_content"].(string)
		if(operate_condition!=""){
			operate_condition=strings.Replace(operate_condition,"\r\n","",-1)
			json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)
			if operateCondJsonMap["conditionType"]!=nil{
				conditionType=operateCondJsonMap["conditionType"].(string)
				lib.Logger.Infof("conditionType=",conditionType)
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
			operate_content=strings.Replace(operate_content,"\r\n","",-1)
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
		// operateScipt
		if operateCondContentJsonMap["operate_script"]!=nil{
			operateScipt=operateCondContentJsonMap["operate_script"].(string)
			lib.Logger.Infof("operateScipt=",operateScipt)
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
			lib.Logger.Infof("conditionFieldKeyValue=",conditionFieldKeyValue)
		}

		//判断条件类型 如果是JUDGE 判断是否存在 如果存在做操作后动作
		// {"operate_type":"UPDATE","pri_key":"id","action_type":"ACC","action_field":"goods_num"}
		operate_type=operateCondContentJsonMap["operate_type"].(string)
		operate_table=operateCondContentJsonMap["operate_table"].(string)
		lib.Logger.Infof("operate_type=",operate_type)
		lib.Logger.Infof("operate_table=",operate_table)
		lib.Logger.Infof("operate_type=",conditionTable)


		if(filter_content!=""){
			filter_content=strings.Replace(filter_content,"\r\n","",-1)
			json.Unmarshal([]byte(filter_content), &filterCondContentJsonMap)
		}
		if filterCondContentJsonMap["filterFunc"]!=nil{
			filterFunc=filterCondContentJsonMap["filterFunc"].(string)
		}
		if filterCondContentJsonMap["filterFields"]!=nil{
			filterFiledArrStr=filterCondContentJsonMap["filterFields"].(string)
			json.Unmarshal([]byte(filterFiledArrStr), &filterFiledArr)
		}

		// filterFieldKey
		if filterCondContentJsonMap["filterFieldKey"]!=nil{
			filterKey=filterCondContentJsonMap["filterFieldKey"].(string)
		}

		var isFiltered bool
		if strings.Contains(filterKey,"="){
			arr:=strings.Split(filterKey,"=")
			field0:=arr[0]
			value0:=arr[1]
			if option.ExtendedMap[field0]==value0{
				isFiltered=true
				break;
			}else{
				isFiltered=false
			}
		}else if filterKey==""{
			isFiltered=true
		}
		// 如果不满足过滤条件 则不执行当前事件
		if !isFiltered{
			continue
		}
		if filterFunc!=""{
			filterFuncSql:="select "+filterFunc+"('"+ConverStrFromMap(filterKey,option.ExtendedMap)+"') as result;"
			filterResult,errorMessage:=api.ExecFuncForOne(filterFuncSql,"result")
			if errorMessage!=nil{
				//tx.Rollback()
			}
			if filterResult!=""{
				continue
			}
		}

		// 前置事件新处理方式   只传参数   逻辑处理在存储过程处理
		if "CASCADE_DELETE"==operate_type|| "UPDATE_MASTER"==operate_type{
			if operateFunc!=""{
				ids:=option.Ids
				for _,item:=range ids{
					operateFuncSql:="select "+operateFunc+"('"+item+"');"
					_,errorMessage:=api.ExecFunc(operateFuncSql)
					lib.Logger.Error("errorMessage=%s",errorMessage)
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
				lib.Logger.Error("errorMessage=%s", errorMessage)
			}else{
				lib.Logger.Infof("rs", rsQuery)
			}
			operate_type:=operateCondContentJsonMap["operate_type"].(string)
			pri_key:=operateCondContentJsonMap["pri_key"].(string)
			var pri_key_value string
			action_type:=operateCondContentJsonMap["action_type"].(string)
			action_field:=operateCondContentJsonMap["action_field"].(string)


			action_field_value1:=option.ExtendedMap[action_field].(float64)
			lib.Logger.Infof("action_field_value1",action_field_value1)
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
							lib.Logger.Infof("err0",err0)
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
					lib.Logger.Infof("rowesAffected=",rowesAffected)
					if error!=nil{
						lib.Logger.Infof("err=",error)
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
				}else{
					// errorMessage.ErrorDescription="api remote error"
				}
			}
			if operateFunc!=""{
				var operateFuncSql string
				params:=ConcatObjectProperties(conditionFiledArr,option.ExtendedMap)
				paramArr:=strings.Split(params,",")
				if len(conditionFiledArr)!=len(paramArr){
					lib.Logger.Error("errorMessage=%s","function "+operateFunc+" params count is not match input")
					continue
				}
				if params!="''"{
					operateFuncSql="select "+operateFunc+"("+params+") as result;"
				}else{
					operateFuncSql="select "+operateFunc+"() as result;"
				}

					result,errorMessage:=api.ExecFuncForOne(operateFuncSql,"result")
					if result!="" && conditionFieldKey!=""{
						option.ExtendedMap[conditionFieldKey]=result
					}
					lib.Logger.Error("errorMessage=%s",errorMessage)


			}
		}
		if "PRE_SYNC"==operate_type{
			if operateFunc!=""{
				var operateFuncSql string
				params:=ConcatObjectProperties(conditionFiledArr,option.ExtendedMap)
				paramArr:=strings.Split(params,",")
				if len(conditionFiledArr)!=len(paramArr){
					lib.Logger.Error("errorMessage=%s","function "+operateFunc+" params count is not match input")
					continue
				}
				if params!="''"{
					operateFuncSql="select "+operateFunc+"("+params+") as result;"
					result,errorMessage:=api.ExecFuncForOne(operateFuncSql,"result")
                    fmt.Print(result,errorMessage)
					if errorMessage!=nil{
						//tx.Rollback()
					}
				}else if len(option.Ids)>0{
					for _,id:=range option.Ids{
						operateFuncSql="select "+operateFunc+"('"+id+"') as result;"
						result,errorMessage:=api.ExecFuncForOne(operateFuncSql,"result")
						fmt.Print(result,errorMessage)
						if errorMessage!=nil{
							//tx.Rollback()
						}
					}

				}



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
				lib.Logger.Error("errorMessage=%s",errorMessage)
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
								if(slaveItem[overFirstValue]!=nil){
									item[overFirstValue]=slaveItem[overFirstValue]
								}

							}

							continue
						}
					}
					params:=ConcatObjectProperties(conditionFiledArr,item)
					paramArr:=strings.Split(params,",")
					if len(conditionFiledArr)!=len(paramArr){
						lib.Logger.Error("errorMessage=%s","function "+operateFunc+" params count is not match input")
						continue
					}
					if params!="''"{
						operateFuncSql="select "+operateFunc+"("+params+") as result;"
					}else{
						operateFuncSql="select "+operateFunc+"() as result;"
					}

					result,errorMessage:=api.ExecFuncForOne(operateFuncSql,"result")
					if result!="" && conditionFieldKey!=""{
						option.ExtendedMap[conditionFieldKey]=result
					}
					lib.Logger.Error("errorMessage=%s",errorMessage)
					if errorMessage!=nil{
						//tx.Rollback()
					}
				}



			}
		}
		if "PRE_EMBED_SCRIPT"==operate_type {
			// 通过自定义脚本前置获取需要入库的数据
			if operateScipt!="" {
				preData,errorMessage:=MutilExec(api,option,conditionFiledArr,nil,operateScipt)
				lib.Logger.Infof("preData=,", preData,"errorMessage=",errorMessage,)
				if len(preData)>0{
					for k,v:=range preData[0]{
						option.ExtendedMap[k]=v
					}
				}
				

			}

		}
	}

	// {"conditionType":"JUDGE","conditionTable":"customer.shopping_cart","conditionFields":"[\"customer_id\",\"goods_id\"]"}
    if data== nil && option.ExtendedMap!=nil{
    	data=append(data,option.ExtendedMap)
	}
	return data,errorMessage;
}


//后置事件处理
func PostEvent(api adapter.IDatabaseAPI,tx *sql.Tx,tableName string ,equestMethod string,data []map[string]interface{},option QueryOption,redisHost string)(rs []map[string]interface{},errorMessage *ErrorMessage){
    //tx,err:=api.Connection().Begin()
	//lib.Logger.Error("err=%s",err)
	operates,errorMessage:=	SelectOperaInfo(api,api.GetDatabaseMetadata().DatabaseName+"."+tableName,equestMethod,"0")
	lib.Logger.Error("errorMessage=%s",errorMessage)

	//	var actionType string


	for _,operate:=range operates {
		var operate_condition string
		var operate_content string
		var filter_content string
		var conditionTable string
		var conditionFileds string
		var conditionFileds1 string
		var resultFileds string
		var operate_type string
		var operate_table string

		var filterFunc string
		var filterFieldKey string
		var operateCondContentJsonMap map[string]interface{}
		var operateCondJsonMap map[string]interface{}
		var operateFilterContentJsonMap map[string]interface{}
		var operateFunc string
		var operateFunc1 string
		var operateScipt string
		var operateProcedure string
		var actionType string
		var conditionFiledArr []string
		var conditionFiledArr1 []string
		var resultFieldsArr []string

		fieldList:=list.New()

		//var conditionComplex string
		operate_condition= operate["operate_condition"].(string)
		operate_content = operate["operate_content"].(string)
		filter_content = operate["filter_content"].(string)
		//var fields []string
		if(operate_condition!=""){
			operate_condition=strings.Replace(operate_condition,"\r\n","",-1)
			json.Unmarshal([]byte(operate_condition), &operateCondJsonMap)

			if operateCondJsonMap["conditionFields"]!=nil{
				conditionFileds=operateCondJsonMap["conditionFields"].(string)
			}
			if operateCondJsonMap["conditionFields1"]!=nil{
				conditionFileds1=operateCondJsonMap["conditionFields1"].(string)
			}
			if operateCondJsonMap["resultFields"]!=nil{
				resultFileds=operateCondJsonMap["resultFields"].(string)
			}
			if operateCondJsonMap["conditionTable"]!=nil{
				conditionTable=operateCondJsonMap["conditionTable"].(string)
			}

			json.Unmarshal([]byte(conditionFileds), &conditionFiledArr)
			json.Unmarshal([]byte(conditionFileds1), &conditionFiledArr1)
			json.Unmarshal([]byte(resultFileds), &resultFieldsArr)
		}
		if(operate_content!=""){
			operate_content=strings.Replace(operate_content,"\r\n","",-1)
			json.Unmarshal([]byte(operate_content), &operateCondContentJsonMap)
		}
		if(filter_content!=""){
			filter_content=strings.Replace(filter_content,"\r\n","",-1)
			json.Unmarshal([]byte(filter_content), &operateFilterContentJsonMap)
		}
		if operateFilterContentJsonMap["filterFunc"]!=nil{
			filterFunc=operateFilterContentJsonMap["filterFunc"].(string)
		}
		if operateFilterContentJsonMap["filterFieldKey"]!=nil{
			filterFieldKey=operateFilterContentJsonMap["filterFieldKey"].(string)
		}

		var isFiltered bool
		if strings.Contains(filterFieldKey,"="){
			arr:=strings.Split(filterFieldKey,"=")
			field0:=arr[0]
			value0:=arr[1]
			if option.ExtendedMap[field0]==value0{
				isFiltered=true
				break;
			}else{
				isFiltered=false
			}
		}else if filterFieldKey==""{
			isFiltered=true
		}
		// 如果不满足过滤条件 则不执行当前事件
		if !isFiltered{
			continue
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
		// operateScipt
		if operateCondContentJsonMap["operate_script"]!=nil{
			operateScipt=operateCondContentJsonMap["operate_script"].(string)
			lib.Logger.Infof("operateScipt=",operateScipt)
		}
		// action_type
		if operateCondContentJsonMap["action_type"]!=nil{
			actionType=operateCondContentJsonMap["action_type"].(string)
			lib.Logger.Infof("actionType=",actionType)
		}
		if operateCondContentJsonMap["operate_func"]!=nil{
			operateFunc=operateCondContentJsonMap["operate_func"].(string)
			lib.Logger.Infof("operateFunc=",operateFunc)
		}
		if operateCondContentJsonMap["operate_func1"]!=nil{
			operateFunc1=operateCondContentJsonMap["operate_func1"].(string)
			lib.Logger.Infof("operateFunc1=",operateFunc1)
		}
// operateProcedure
		if operateCondContentJsonMap["operate_procedure"]!=nil{
			operateProcedure=operateCondContentJsonMap["operate_procedure"].(string)
			lib.Logger.Infof("operateProcedure=",operateProcedure)
		}
		//if operateCondJsonMap["conditionComplex"]!=nil{
		//	conditionComplex=operateCondJsonMap["conditionComplex"].(string)
		//}
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
				conditionFieldKeyValue=InterToStr(option.ExtendedMap[conditionFieldKey])
			}
		}
		//判断条件类型 如果是JUDGE 判断是否存在 如果存在做操作后动作
		// {"operate_type":"UPDATE","pri_key":"id","action_type":"ACC","action_field":"goods_num"}
		if operateCondContentJsonMap["operate_type"]!=nil{
			operate_type=operateCondContentJsonMap["operate_type"].(string)
		}
		if operateCondContentJsonMap["operate_table"]!=nil{
			operate_table=operateCondContentJsonMap["operate_table"].(string)
			fmt.Print(operate_table)
		}

		//actionType=operateCondContentJsonMap["action_type"].(string)
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
				lib.Logger.Infof("item=",item)
				farm_type_o:=item["farm_type"]

				if farm_type_o==nil {
					farm_type="1"
				}else{
					farm_type=farm_type_o.(string)
				}

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
				lib.Logger.Error("errorMessage=%s", errorMessage)
			}else{
				lib.Logger.Infof("rs", rsQuery)
			}
			for _,item:=range rsQuery{
				// 先拦截器校验
				if filterFunc!=""{

					filterFuncSql:="select "+filterFunc+"('"+ConverStrFromMap(filterFieldKey,option.ExtendedMap)+"') as result;"
					filterResult,errorMessage:=api.ExecFuncForOne(filterFuncSql,"result")
					if errorMessage!=nil{
						//tx.Rollback()
					}

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
					lib.Logger.Error("errorMessage=%s",errorMessage)
					// 先删掉 已经计算出来的得分记录  然后重新计算
					deleteWhere:=make(map[string]interface{})
					deleteWhere[conditionFieldKey]=conditionFieldKeyValue
					deleteWhere["credit_level_model_id"]=item["credit_level_model_id"]
					_,errorMessage=api.Delete("credit_level_customer",nil,deleteWhere)
					lib.Logger.Error("errorMessage=%s",errorMessage)
					for _,rsQueryPatchItem:=range rsQueryPatch{
						CallLevel(api,item,extraOperateMap,rsQueryPatchItem,conditionFieldKeyValue)
					}

				}else{

					CallLevel(api,item,extraOperateMap,option.ExtendedMap,conditionFieldKeyValue)

				}



			}

		}
		if "SYNC"==operate_type {
			if operateFunc!=""{
				if conditionFieldKeyValue!=""{
					operateFuncSql:="select "+operateFunc+"('"+conditionFieldKeyValue+"') as result;"
					rs,error:=api.ExecSqlWithTx(operateFuncSql,tx)
					lib.Logger.Infof("result=",rs)
					if error!=nil{
						lib.Logger.Error("error=%s",error.Error())
						errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,error.Error()}
						tx.Rollback()
					}

					//if result!="" && conditionFieldKey!=""{
					//	option.ExtendedMap[conditionFieldKey]=result
					//}



				}else if len(conditionFiledArr)>0{
					paramsFunc:=ConcatObjectProperties(conditionFiledArr,option.ExtendedMap)
					paramArr:=strings.Split(paramsFunc,",")
					if len(conditionFiledArr)!=len(paramArr){
						lib.Logger.Error("errorMessage=%s","function "+operateFunc+" params count is not match input")
						continue
					}
					if paramsFunc!=""{
						operateFuncSql:="select "+operateFunc+"("+paramsFunc+");"
						result,error:=api.ExecSqlWithTx(operateFuncSql,tx)
						if error!=nil{
							lib.Logger.Error("result=%s,error=%s",result,error.Error())
							errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,error.Error()}
							tx.Rollback()
						}

					}
				}


			}
			if operateProcedure!=""{
				if conditionFieldKeyValue!=""{
					operateProcedureSql:="CALL "+operateProcedure+"('"+conditionFieldKeyValue+"');"
					result,errorMessage:=api.ExecFuncForOne(operateProcedureSql,"result")
					lib.Logger.Infof("result=",result)
					lib.Logger.Error("errorMessage=%s",errorMessage)


				}else if len(conditionFiledArr)>0{
					paramsPro:=ConcatObjectProperties(conditionFiledArr,option.ExtendedMap)
					if paramsPro!=""{
						operateProcedureSql:="CALL "+operateProcedure+"("+paramsPro+");"
						result,errorMessage:=api.ExecFuncForOne(operateProcedureSql,"result")
						lib.Logger.Infof("result=",result)
						lib.Logger.Error("errorMessage=%s",errorMessage)

					}
				}

			}
		}
		if "SYNC_COMPLEX"==operate_type{
			if operateFunc!=""{
				var syncComplexOption QueryOption
				syncComplexOption.Table=tableName
				complexWhereMap:= map[string]WhereOperation{}
				complexWhereMap[tableName+"."+conditionFieldKey]=WhereOperation{
					"eq",
					option.ExtendedMap[conditionFieldKey],
				}

				syncComplexOption.Wheres=complexWhereMap
				syncComplexData, errorMessage:= api.Select(syncComplexOption)
				fmt.Print("syncComplexData=",syncComplexData)
				fmt.Print(errorMessage)
				for _,item:=range syncComplexData{
					operateFuncSql:="select "+operateFunc+"('"+item["id"].(string)+"') as result;"
					result,errorMessage:=api.ExecFuncForOne(operateFuncSql,"result")
					lib.Logger.Infof("result,errorMessage",result,errorMessage)

				}

			}
		}
		if "EMBED_SCRIPT"==operate_type {
			// actionType SINGLE_PROCESS   MUTIL_PROCESS   MUTIL_PROCESS_FOR
			// SINGLE_PROCESS 单条sql处理
			// MUTIL_PROCESS  多条sql处理 无变量依赖
			// MUTIL_PROCESS_COMPLEX  多条sql处理  有变量依赖

			if operateScipt==""  {
				continue
			}
			if actionType=="SINGLE_PROCESS"{
				result,errorMessage:=SingleExec(api,option,conditionFiledArr,operateScipt)
				if result!="" && conditionFieldKey!="" && errorMessage.ErrorDescription!=""{
					option.ExtendedMap[conditionFieldKey]=result
				}
			}
			if actionType=="MUTIL_PROCESS"{
				operateSciptArr:=strings.Split(operateScipt,";")
				for _,itemScript:=range operateSciptArr{
					result,errorMessage:=SingleExec(api,option,conditionFiledArr,itemScript)
					lib.Logger.Infof("result=",result,"errorMessage=",errorMessage)
				}
			}
			if actionType=="MUTIL_PROCESS_COMPLEX"{
				// 存放script的变量和计算出的值
				varMap:=make(map[string]interface{})
				operateSciptArr :=strings.Split(operateScipt,"!!")
				for _,itemScript:=range operateSciptArr{
					// 解析itemScript
					// 解析模块类型 是赋值还还是返回值类型
					// 判断类型 /*JUDGE*/  for循坏类型 /*FOR_HANDLE*/  while循坏类型 /*WHILE_HANDLE*/  复杂逻辑在一个function里处理(这个方法里没有包含分表的表操作)
					// 赋值类型 /*ASS_VAR*/  同步类型/*SYNC_HANDLE*/  返回类型  /*RETURN_HANDLE*/
					// 如果有判断类型(判断的同时会有赋值类型和同步类型和返回类型)  这里默认判断nil  单个判断/*JUDGE_SINGLE*/  多个判断且/*JUDGE_AND*/  多个判断或/*JUDGE_OR*/
					if strings.Contains(itemScript,"/*JUDGE_SINGLE*/"){
						// /*ASS_VAR*//*JUDE_SINGLE*/$Stest$E SET maxNo=(SELECT MAX(`stu_no`) AS result FROM test.`stu`);
						varParam:=util.GetBetweenStr(itemScript,"$S","$E")
						if varMap[varParam]!=nil && varMap[varParam]!=""{
							itemScript=strings.Replace(itemScript,"/*JUDGE_SINGLE*/$S"+varParam+"$E","",-1)
						}else{
							continue
						}
					}
					if strings.Contains(itemScript,"/*JUDGE_AND*/"){
						// /*ASS_VAR*//*JUDGE_AND*/$Stest-tt$E SET maxNo=(SELECT MAX(`stu_no`) AS result FROM test.`stu`);
						varParamStr:=util.GetBetweenStr(itemScript,"$S","$E")
						paramArr:=strings.Split(varParamStr,"-")
						var param0 string
						var param1 string
						if len(paramArr)>0{
							param0=paramArr[0]
							param1=paramArr[1]
						}
						if varMap[param0]!=nil && varMap[param0]!="" && varMap[param1]!=nil && varMap[param1]!=""{
							itemScript=strings.Replace(itemScript,"/*JUDGE_AND*/$S"+varParamStr+"$E","",-1)
						}else{
							continue
						}
					}
					if strings.Contains(itemScript,"/*JUDGE_OR*/"){
						// /*ASS_VAR*//*JUDGE_OR*/$Stest-tt$E SET maxNo=(SELECT MAX(`stu_no`) AS result FROM test.`stu`);
						varParamStr:=util.GetBetweenStr(itemScript,"$S","$E")
						paramArr:=strings.Split(varParamStr,"-")
						var param0 string
						var param1 string
						if len(paramArr)>0{
							param0=paramArr[0]
							param1=paramArr[1]
						}
						if (varMap[param0]!=nil && varMap[param0]!="") || (varMap[param1]!=nil && varMap[param1]!=""){
							itemScript=strings.Replace(itemScript,"/*JUDGE_OR*/$S"+varParamStr+"$E","",-1)
						}else{
							continue
						}
					}

					if strings.Contains(itemScript,"/*ASS_VAR*/"){
						itemScript=strings.Replace(itemScript,"/*ASS_VAR*/","",-1)
						// 赋值类型： 一个变量赋值  多个变量赋值


						assVarArr:=strings.Split(itemScript,"INTO")
						if len(assVarArr)>1 {
							// INTO 方式赋值
							// SELECT stu_no,project_name into stuNo,projectName from test.stu_score  替换为
							// SELECT stu_no as stuNo, project_name as projectName from test.stu_score
							var execSql  strings.Builder
							execSql.WriteString("SELECT ")
							intoStr:=assVarArr[1]
							assVarStr:=strings.Replace(assVarArr[0],"SELECT","",-1)
							assVarStr=strings.Trim(assVarStr," ")
							fieldArr:=strings.Split(assVarStr,",")
							//取into字段

							fromIndex:=strings.Index(intoStr,"FROM")
							var endStr string
							if fromIndex>0{
								endStr=intoStr[fromIndex:]
								intoStr=intoStr[:fromIndex]
							}

							intoArr:=strings.Split(intoStr,",")

							for i,item:=range intoArr{
								// 去掉前后空格
								varItemTrim:=strings.Trim(fieldArr[i]," ")
								intoItemTrim:=strings.Trim(item," ")
								execSql.WriteString(varItemTrim)
								execSql.WriteString(" as ")
								execSql.WriteString(intoItemTrim)
							    if i<(len(intoArr)-1){
									execSql.WriteString(",")
								}

							}
							execSql.WriteString(" "+endStr)
							//
							result,errorMessage:=MutilExec(api,option,conditionFiledArr,varMap,execSql.String())
							lib.Logger.Error("errorMessage=%s",errorMessage)
							for _,item:=range intoArr{
									key:=strings.Trim(item," ")
									var value string
									if len(result)>0{
										value=InterToStr(result[0][key])
									}else{
										value=""
									}

									varMap[key]=value
								}
							}
						if len(assVarArr)==1 {
							// INTO 方式赋值
							// SELECT stu_no,project_name into stuNo,projectName from test.stu_score  替换为
							// SELECT stu_no as stuNo, project_name as projectName from test.stu_score
							var execSql  strings.Builder
							assVarStr:=strings.Replace(assVarArr[0],"SELECT","",-1)
							fromIndex:=strings.Index(assVarStr,"FROM")
							assVarStr=assVarStr[0:fromIndex]
							assVarStr=strings.Trim(assVarStr," ")
							fieldArr:=strings.Split(assVarStr,",")
							//取into字段

							execSql.WriteString(itemScript)
							//
							result,errorMessage:=MutilExec(api,option,conditionFiledArr,varMap,execSql.String())
							lib.Logger.Error("errorMessage=%s",errorMessage)
							for _,item:=range fieldArr{
								key:=strings.Trim(item," ")
								if strings.Contains(key,"."){
									key=key[strings.Index(key,"."):]
								}
								var value string
								if len(result)>0{
									value=InterToStr(result[0][key])
								}else{
									value=""
								}

								varMap[key]=value
							}
						}
						}
					if strings.Contains(itemScript,"/*ASS_VAR_FROM_FUNCS*/"){
						itemScript=strings.Replace(itemScript,"/*ASS_VAR_FROM_FUNCS*/","",-1)
						// 赋值类型： 一个变量赋值  多个变量赋值

						assVarArr:=strings.Split(itemScript,"INTO")
						if len(assVarArr)>1 {
							// INTO 方式赋值
							// SELECT stu_no,project_name into stuNo,projectName from test.stu_score  替换为
							// SELECT stu_no as stuNo, project_name as projectName from test.stu_score
							var execSql  strings.Builder
							execSql.WriteString(strings.Replace(itemScript,"INTO","AS",-1))
							intoStr:=assVarArr[1]
							intoStr=strings.Replace(intoStr,";","",-1)
							intoStr=strings.Trim(intoStr," ")

							//
							result,errorMessage:=MutilExec(api,option,conditionFiledArr,varMap,execSql.String())
							lib.Logger.Error("errorMessage=%s",errorMessage)
							var value string
							if len(result)>0{
								value=InterToStr(result[0][intoStr])
							}else{
								value=""
							}
							varMap[intoStr]=value
						}
					}


					//  同步类型/*SYNC_HANDLE*/
					if strings.Contains(itemScript,"/*SYNC_HANDLE*/"){
						itemScript=strings.Replace(itemScript,"/*SYNC_HANDLE*/","",-1)
						//result,errorMessage:=SingleExec1(api,option,conditionFiledArr,varMap,itemScript)
						result,error:=ExecWithTx(api,tx,option,conditionFiledArr,varMap,itemScript)
						if error!=nil{
							lib.Logger.Infof("result=",result," errorMessage=",error.Error())
							errorMessage = &ErrorMessage{ERR_SQL_EXECUTION,error.Error()}
							tx.Rollback()
						}

					}
					//  返回类型  /*RETURN_HANDLE*/
					if strings.Contains(itemScript,"/*RETURN_HANDLE*/"){
						varParam:=strings.Replace(itemScript,"RETURN","",-1)
						varParam=strings.Replace(varParam,"(","",-1)
						varParam=strings.Replace(varParam,");","",-1)
						if varMap[varParam]!=nil && varMap[varParam]!=""{
							option.ExtendedMap[conditionFieldKey]=varMap[varParam]
						}
					}





				}
			}



		}
		if "PUSH_MES"==operate_type{
			client := &http.Client{}
			var title string
			var content string
			var userIds string
			var msgType int
			var err error
			if operateCondContentJsonMap["msg_type"]!=nil{
				msgType, err = strconv.Atoi(operateCondContentJsonMap["msg_type"].(string))
				lib.Logger.Infof("msgType=",msgType)
				if err!=nil{
					lib.Logger.Infof("err=",err.Error())
				}
			}
			if operateCondContentJsonMap["title"]!=nil{
				title=operateCondContentJsonMap["title"].(string)
				lib.Logger.Infof("title=",title)
			}
			// 构造请求参数
			userIdsParam:=ConcatObjectProperties(conditionFiledArr,option.ExtendedMap)
			contentParam:=ConcatObjectProperties(conditionFiledArr1,option.ExtendedMap)
			if operateFunc!=""&&userIdsParam!=""{
				userIdsFuncSql:="select "+operateFunc+"("+userIdsParam+") as result;"
				userIds,errorMessage=api.ExecFuncForOne(userIdsFuncSql,"result")
				lib.Logger.Infof("userIds=",userIds)
				lib.Logger.Error("errorMessage=%s",errorMessage)
				errorMessage=errorMessage

			}
			if operateFunc1!=""&&contentParam!=""{
				conctentFuncSql:="select "+operateFunc1+"("+contentParam+") as result;"
				content,errorMessage=api.ExecFuncForOne(conctentFuncSql,"result")
				lib.Logger.Infof("content=",content)
				lib.Logger.Error("errorMessage=%s",errorMessage)
				errorMessage=errorMessage

			}
			pushParamMap:=make(map[string]interface{})
			pushParamMap["senderId"]=1
			pushParamMap["senderName"]="SYSTEM"
			pushParamMap["userIds"]=userIds
			pushParamMap["msgType"]=msgType
			pushParamMap["title"]=title
			pushParamMap["content"]=content
			pushParamMap["timestamp"]=time.Now().Format("2006-01-02 15:04:05")
			pushParamMapBytes,err:=json.Marshal(pushParamMap)
			fmt.Print("err",err)
			reqest, err := http.NewRequest("POST", operate_table, bytes.NewBuffer(pushParamMapBytes))
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

			}else{
				// errorMessage.ErrorDescription="api remote error"
			}

		}
// limit
	}
	if data== nil && option.ExtendedMap!=nil{
		data=append(data,option.ExtendedMap)
	}
	// {"conditionType":"JUDGE","conditionTable":"customer.shopping_cart","conditionFields":"[\"customer_id\",\"goods_id\"]"}

	return data,errorMessage;
}
func CallLevel(api adapter.IDatabaseAPI,item map[string]interface{},extraOperateMap map[string]interface{},extendMap map[string]interface{},conditionFieldKeyValue string){
	//tx,err:=api.Connection().Begin()
	//fmt.Print("//tx-error",err)
	lib.Logger.Infof("item=",item)
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
				result,errorMesssage:=api.ExecFuncForOne(pre_operate_func_sql,"result")
				fmt.Print(result,errorMesssage)

			}
			if result==""{
				result=ConverStrFromMap(second_level_norm_arr[0],extendMap)
			}

		}
	}else if len(second_level_norm_arr)>1{
		// 如果二级指标有多个字段参与计算
		//lib.Logger.Infof("judge_type=",judge_type," judge_content=",judge_content)
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
			result,errorMesssage:=api.ExecFuncForOne(pre_operate_func_sql,"result")
			fmt.Print(result,errorMesssage)

		}

	}

	//lib.Logger.Error("errorMessage=%s",errorMessage)
	operate_func_sql:="select "+operate_func+"('"+result+"','"+conditionFieldKeyValue+"','"+credit_level_model_id+"') as result;"
	result1,errorMessage:=api.ExecFuncForOne(operate_func_sql,"result")

	lib.Logger.Infof("result1,errorMesssage",result1,errorMessage)

}
func CallFunc(api adapter.IDatabaseAPI,calculate_field string,calculate_func string,paramStr string,asyncObjectMap map[string]interface{})(map[string]interface{}){
	//tx,err:=api.Connection().Begin()
	//fmt.Print("//tx-error",err)
	if strings.Contains(calculate_field,","){
		fields:=strings.Split(calculate_field,",")
		for index,item:=range fields{
			calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
			result,errorMessage:=api.ExecFuncForOne(calculate_func_sql_str,"result")
			//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")

			fmt.Print("errorMessage",errorMessage)
			if result==""{
				result="0"
			}
			asyncObjectMap[item]=result
		}
	}
	return asyncObjectMap
}

func CalculatePre(api adapter.IDatabaseAPI,repeatItem map[string]interface{},funcParamFields []string,pre_subject_key string,operate_func string){
	//tx,err:=api.Connection().Begin()
	//fmt.Print("//tx-error",err)
	//	asyncObjectMap=BuildMapFromBody(conditionFiledArr,masterInfoMap,asyncObjectMap)
	//asyncObjectMap=BuildMapFromBody(conditionFiledArr1,slave,asyncObjectMap)
	//slave["subject_key_pre"]=slave["subject_key"]
	////lib.Logger.Infof("operate_table",operate_table)
	//lib.Logger.Infof("calculate_field",calculate_field)
	//lib.Logger.Infof("calculate_func",calculate_func)

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
			result,errorMessage:=api.ExecFuncForOne(operate_func_sql,"result")
			fmt.Print("errorMessage",errorMessage)
			if errorMessage!=nil{
				//tx.Rollback()
			}
			lib.Logger.Infof("operate_func_sql-result",result)
		}




	}

}
func InterToStr(fieldInter interface{})(string){
	var result string
	switch fieldInter.(type){
	case string: result=fieldInter.(string)
	case int:result=strconv.Itoa(fieldInter.(int))
	// int64
	case int64:result= strconv.FormatInt(fieldInter.(int64),10)
	case float64: result=strconv.FormatFloat(fieldInter.(float64),'f',-1,64)
	}
	return result
}
func InterToInt(fieldInter interface{})(int64){
	var result int64
	switch fieldInter.(type){
	case string: result, _ = strconv.ParseInt(fieldInter.(string), 10, 64)
	case int:result=fieldInter.(int64)
		// int64
	case int64:result= fieldInter.(int64)

	}
	return result
}

func SingleExec(api adapter.IDatabaseAPI,option QueryOption,conditionFiledArr []string,operateScipt string)(result string,errorMessage *ErrorMessage){
	for _,itemField:=range conditionFiledArr{
		operateScipt=strings.Replace(operateScipt,"${"+itemField+"}","'"+InterToStr(option.ExtendedMap[itemField])+"'",-1)
	}

	//lib.Logger.Infof("operateScipt=", operateScipt)
	result,errorMessage=api.ExecFuncForOne(operateScipt,"result")
	//lib.Logger.Infof("result=,", result,"errorMessage=",errorMessage,)
	return
}
func SingleExec1(api adapter.IDatabaseAPI,option QueryOption,conditionFiledArr []string,varMap map[string]interface{},operateScipt string)(result string,errorMessage *ErrorMessage){
	for _,itemField:=range conditionFiledArr{
		operateScipt=strings.Replace(operateScipt,"${"+itemField+"}","'"+InterToStr(option.ExtendedMap[itemField])+"'",-1)
	}
	for k,v :=range varMap{
		operateScipt=strings.Replace(operateScipt,"${"+k+"}","'"+InterToStr(v)+"'",-1)
	}
	//lib.Logger.Infof("operateScipt=", operateScipt)
	result,errorMessage=api.ExecFuncForOne(operateScipt,"result")
	//lib.Logger.Infof("result=,", result,"errorMessage=",errorMessage,)
	if errorMessage!=nil{
		lib.Logger.Infof("operateScipt=,", operateScipt,"errorMessage=",errorMessage,)
	}
	return
}

func MutilExec(api adapter.IDatabaseAPI,option QueryOption,conditionFiledArr []string,varMap map[string]interface{},operateScipt string)(result []map[string]interface{},errorMessage *ErrorMessage){
	for _,itemField:=range conditionFiledArr{
		operateScipt=strings.Replace(operateScipt,"${"+itemField+"}","'"+InterToStr(option.ExtendedMap[itemField])+"'",-1)
	}
	for k,v :=range varMap{
		operateScipt=strings.Replace(operateScipt,"${"+k+"}","'"+InterToStr(v)+"'",-1)
	}

	result,errorMessage=api.ExecSql(operateScipt)
	if errorMessage!=nil{
		lib.Logger.Infof("operateScipt=,", operateScipt,"errorMessage=",errorMessage,)
	}

	return
}
func ExecWithTx(api adapter.IDatabaseAPI,tx *sql.Tx,option QueryOption,conditionFiledArr []string,varMap map[string]interface{},operateScipt string)(result sql.Result,error error){
	for _,itemField:=range conditionFiledArr{
		operateScipt=strings.Replace(operateScipt,"${"+itemField+"}","'"+InterToStr(option.ExtendedMap[itemField])+"'",-1)
	}
	for k,v :=range varMap{
		operateScipt=strings.Replace(operateScipt,"${"+k+"}","'"+InterToStr(v)+"'",-1)
	}
	lib.Logger.Infof("operateScipt=", operateScipt)
	//result,errorMessage=api.ExecFuncForOne(operateScipt,"result")

	result,error=tx.Exec(operateScipt)

	//lib.Logger.Infof("result=,", result,"errorMessage=",errorMessage,)
	if error!=nil{
		lib.Logger.Infof("operateScipt=,", operateScipt,"errorMessage=",error.Error())
	}
	return
}
