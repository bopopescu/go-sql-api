package mysql

import (
	"strings"
	"fmt"
	"encoding/json"
	"strconv"
	"github.com/shiyongabc/go-sql-api/adapter"
	."github.com/shiyongabc/go-sql-api/types"

	"github.com/satori/go.uuid"
	"time"
)

// 异步执行
func AsyncFunc(api adapter.IDatabaseAPI,repeatCalculateData,operates []map[string]interface{},calpreMap map[string]interface{},isCalPre bool,index int,c chan int){
	tx,err:=api.Connection().Begin()
	fmt.Print("tx-error",err)
	var operate_type string
	var operate_table string
	var calculate_field string
	var calculate_func string
	var conditionFileds string
	var conditionFileds1 string
	var funcParamFieldStr string
	var operateCondJsonMap map[string]interface{}
	var operateCondContentJsonMap map[string]interface{}


	var conditionFiledArr []string
	var conditionFiledArr1 []string
	//conditionFiledArr := list.New()
	//conditionFiledArr1 := list.New()
	var funcParamFields []string
	var operate_func string
	fmt.Printf("async-task-begin=",time.Now().Format("2006-01-02 15:04:05"))
	for _,repeatItem:=range repeatCalculateData{


		id:=repeatItem["id"]
		//	accountYear:=repeatItem["account_period_year"]
		fmt.Printf("id=",id)

		//  删掉 本期合计 本年累计  重新计算
		// order_num为空说明是累计数

		//var repeatOrderNum int
		var repeatAccountPeriodNum string
		var repeatAccountYear string
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

			if repeatItem["order_num"]!=nil{
				repeatOrderNum, err := strconv.Atoi(repeatItem["order_num"].(string))
				if err!=nil{
					fmt.Printf("err=",err,"repeatOrderNum=",repeatOrderNum)
				}
			}
			// repeatAccountPeriodNum
			if repeatItem["account_period_num"]!=nil{
				repeatAccountPeriodNum=repeatItem["account_period_num"].(string)

				fmt.Printf("repeatAccountPeriodNum=",repeatAccountPeriodNum)

			}
			if repeatItem["account_period_year"]!=nil{
				repeatAccountYear=repeatItem["account_period_year"].(string)

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

					result,errorMessage:=api.ExecFuncForOne(calculate_func_sql_str,"result")
					if result==""{
						result="0"
					}
					if errorMessage!=nil{
						// tx.Rollback()
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


				in_subject_key:=paramsMap["subject_key"].(string)
				in_farm_id:=paramsMap["farm_id"].(string)
				obtianPreSubjectSql:="select obtainPreSubjectKey('"+in_subject_key+"','"+in_farm_id+"'"+") as pre_subject_key;"
				pre_subject_key,errorMessage:=api.ExecFuncForOne(obtianPreSubjectSql,"pre_subject_key")

				asyncObjectMap["subject_key_pre"]=pre_subject_key

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

				asyncObjectMap["subject_key_pre"]=repeatItem["subject_key"]


			}
			account_period_num:=repeatItem["account_period_num"]
			// ASYNC_BATCH_SAVE_BEGIN_PEROID 计算期初
			if "ASYNC_BATCH_SAVE_BEGIN_PEROID"==operate_type {
				asyncObjectMap=BuildMapFromBody(conditionFiledArr,repeatItem,asyncObjectMap)
				asyncObjectMap=BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

				fmt.Printf("operate_table",operate_table)
				fmt.Printf("calculate_field",calculate_field)
				fmt.Printf("calculate_func",calculate_func)
				var paramStr string
				paramsMap:=make(map[string]interface{})
				periodNum,_:=strconv.Atoi(account_period_num.(string))
				beginLineNum:=0-periodNum
				// funcParamFields
				if calculate_func!=""{
					// SELECT CONCAT(DATE_FORMAT(NOW(),'%Y-%m'),'-01') as first_date;
					laste_date_sql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y-%m'),'-01') AS first_date;"
					result1,errorMessage:=api.ExecFuncForOne(laste_date_sql,"first_date")
					fmt.Print(errorMessage)
					//masterInfoMap["account_period_year"]=result1
					beginYearSql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y'),'-01-01') AS beginYear;"
					beginYearResult,errorMessage:=api.ExecFuncForOne(beginYearSql,"beginYear")
					lastDaySql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y'),'-01-31') AS lastDay;"
					lastDayResult,errorMessage:=api.ExecFuncForOne(lastDaySql,"lastDay")

					asyncObjectMap["voucher_type"]=nil
					asyncObjectMap["line_number"]=beginLineNum
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
					id0,errorMessage:=api.ExecFuncForOne(judgeExistsSql,"id")

					judgeExistsSqlSub:="select judgeSubjectPeroidExists("+paramStr+") as id1;"
					idSub,errorMessage:=api.ExecFuncForOne(judgeExistsSqlSub,"id1")
					if strings.Contains(calculate_field,","){
						fields:=strings.Split(calculate_field,",")
						for index,item:=range fields{
							calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
							result,errorMessage:=api.ExecFuncForOne(calculate_func_sql_str,"result")
							//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
							if result==""{
								result="0"
							}
							if errorMessage!=nil{
								// tx.Rollback().Rollback()
							}
							asyncObjectMap[item]=result

						}
					}


					asyncObjectMap["subject_key_pre"]=repeatItem["subject_key"]
					if id0==""{
						if idSub!=""{
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
					judgeNeedUpdateLatestKnotsId,errorMessage:=api.ExecFuncForOne(judgeNeedUpdateLatestKnotsSql,"id")
					//var latestKnotsFunds string
					asyncObjectMap["summary"]="上年结转"
					asyncObjectMap["line_number"]="-1"
					asyncObjectMap["order_num"]=0
					asyncObjectMap["account_period_year"]=beginYearResult
					asyncObjectMap["account_period_num"]="1"
					asyncObjectMap["id"]=judgeNeedUpdateLatestKnotsId
					paramsMap["account_period_num"]="1"
					paramsMap["account_period_year"]=beginYearResult
					paramStr=ConcatObjectProperties(funcParamFields,paramsMap)
					asyncObjectMap=CallFunc(api,calculate_field,calculate_func,paramStr,asyncObjectMap)
					if result1!=beginYearResult && judgeNeedUpdateLatestKnotsId!=""{
						r,errorMessage:= api.Update(operate_table,judgeNeedUpdateLatestKnotsId,asyncObjectMap)
						if errorMessage!=nil{
							fmt.Printf("errorMessage=",errorMessage)
						}
						fmt.Printf("rs=",r)
					}else if result1!=beginYearResult && judgeNeedUpdateLatestKnotsId==""{
						asyncObjectMap["id"]=uuid.NewV4().String()+"-beginperoid-knots"
						r,errorMessage:= api.Create(operate_table,asyncObjectMap)
						if errorMessage!=nil{
							fmt.Printf("errorMessage=",errorMessage)
						}
						fmt.Printf("rs=",r)
					}

					// 如果当期不是1 且第一期没有凭证 更新上年结转 本期合计  本年累计 judgeNeedUpdateLatestKnots
					judgeNeedUpdateLatestKnotsSqlCureent:="select judgeNeedUpdateLatestKnots("+paramStr+",'2') as id;"
					judgeNeedUpdateLatestKnotsIdCurrent,errorMessage:=api.ExecFuncForOne(judgeNeedUpdateLatestKnotsSqlCureent,"id")
					asyncObjectMap["summary"]="本期合计"
					asyncObjectMap["line_number"]=100
					asyncObjectMap["order_num"]=0
					asyncObjectMap["account_period_year"]=lastDayResult
					asyncObjectMap["account_period_num"]="1"
					asyncObjectMap["debit_funds"]="0"
					asyncObjectMap["credit_funds"]="0"

					asyncObjectMap["id"]=judgeNeedUpdateLatestKnotsIdCurrent
					if result1!=beginYearResult && judgeNeedUpdateLatestKnotsIdCurrent!=""{
						r,errorMessage:= api.Update(operate_table,judgeNeedUpdateLatestKnotsIdCurrent,asyncObjectMap)
						if errorMessage!=nil{
							fmt.Printf("errorMessage=",errorMessage)
						}
						fmt.Printf("rs=",r)
					}else if result1!=beginYearResult && judgeNeedUpdateLatestKnotsIdCurrent==""{
						asyncObjectMap["id"]=uuid.NewV4().String()+"-beginperoid-knots-period"
						r,errorMessage:= api.Create(operate_table,asyncObjectMap)
						if errorMessage!=nil{
							fmt.Printf("errorMessage=",errorMessage,"r=",r)
						}
					}
					// 如果当期不是1 且第一期没有凭证 更新上年结转 本期合计  本年累计 judgeNeedUpdateLatestKnots
					judgeNeedUpdateLatestKnotsSqlYear:="select judgeNeedUpdateLatestKnots("+paramStr+",'3') as id;"
					judgeNeedUpdateLatestKnotsIdYear,errorMessage:=api.ExecFuncForOne(judgeNeedUpdateLatestKnotsSqlYear,"id")
					asyncObjectMap["summary"]="本年累计"
					asyncObjectMap["line_number"]=101
					asyncObjectMap["order_num"]=0
					asyncObjectMap["account_period_year"]=lastDayResult
					asyncObjectMap["account_period_num"]="1"
					asyncObjectMap["debit_funds"]="0"
					asyncObjectMap["credit_funds"]="0"


					asyncObjectMap["id"]=judgeNeedUpdateLatestKnotsIdYear
					if result1!=beginYearResult && judgeNeedUpdateLatestKnotsIdYear!=""{
						r,errorMessage:= api.Update(operate_table,judgeNeedUpdateLatestKnotsIdYear,asyncObjectMap)
						if errorMessage!=nil{
							fmt.Printf("errorMessage=",errorMessage)
						}
						fmt.Printf("rs=",r)
					}else if result1!=beginYearResult && judgeNeedUpdateLatestKnotsIdYear==""{
						asyncObjectMap["id"]=uuid.NewV4().String()+"-beginperoid-knots-year"
						r,errorMessage:= api.Create(operate_table,asyncObjectMap)
						if errorMessage!=nil{
							fmt.Printf("errorMessage=",errorMessage,"r=",r)
						}
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
					result1,errorMessage:=api.ExecFuncForOne(laste_date_sql,"last_date")
					//masterInfoMap["account_period_year"]=result1
                    if errorMessage!=nil{
                    	tx.Rollback()
					}
					asyncObjectMap["voucher_type"]=nil
					asyncObjectMap["line_number"]=100
					asyncObjectMap["order_num"]=100
					asyncObjectMap["summary"]="本期合计"
					asyncObjectMap["account_period_year"]=result1
					//如果执行方法不为空 执行配置中方法
					paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
					paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
					//把对象的所有属性的值拼成字符串
					paramStr=ConcatObjectProperties(funcParamFields,paramsMap)




					if strings.Contains(calculate_field,","){
						fields:=strings.Split(calculate_field,",")
						for index,item:=range fields{
							calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
							result,errorMessage:=api.ExecFuncForOne(calculate_func_sql_str,"result")
							//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
							if result==""{
								result="0"
							}
							if errorMessage!=nil{
								tx.Rollback()
							}
							asyncObjectMap[item]=result

						}
					}



					// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
					judgeExistsSql:="select judgeCurrentPeroidExists("+paramStr+") as id;"

					id0,errorMessage:=api.ExecFuncForOne(judgeExistsSql,"id")

					judgeExistsSqlSub:="select judgeSubjectPeroidExists("+paramStr+") as id1;"

					idSub,errorMessage:=api.ExecFuncForOne(judgeExistsSqlSub,"id1")
					asyncObjectMap["subject_key_pre"]=repeatItem["subject_key"]

					if id0==""{
						if idSub!=""{
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
					result1,errorMessage:=api.ExecFuncForOne(laste_date_sql,"last_date")
					//masterInfoMap["account_period_year"]=result1
                    if errorMessage!=nil{
                    	tx.Rollback()
					}
					asyncObjectMap["voucher_type"]=nil
					asyncObjectMap["order_num"]=101
					asyncObjectMap["line_number"]=101
					asyncObjectMap["summary"]="本年累计"
					asyncObjectMap["account_period_year"]=result1
					//如果执行方法不为空 执行配置中方法
					paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
					//paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
					//把对象的所有属性的值拼成字符串
					paramStr=ConcatObjectProperties(funcParamFields,paramsMap)


					if strings.Contains(calculate_field,","){
						fields:=strings.Split(calculate_field,",")
						for index,item:=range fields{
							calculate_func_sql_str:="select ROUND("+calculate_func+"("+paramStr+",'"+strconv.Itoa(index+1)+"'"+"),2) as result;"
							result,errorMessage:=api.ExecFuncForOne(calculate_func_sql_str,"result")
							//rs,error:= api.ExecFunc("SELECT ROUND(calculateBalance('101','31bf0e40-5b28-54fc-9f15-d3e49cf595c1','005ef4c0-f188-4dec-9efb-f3291aefc78a'),2) AS result; ")
							if result==""{
								result="0"
							}
							if errorMessage!=nil{
								tx.Rollback()
							}
							asyncObjectMap[item]=result

						}
					}


					asyncObjectMap["subject_key_pre"]=repeatItem["subject_key"]

					// 先判断是否已经存在当期累计数据  如果存在 更新即可  否则 新增
					judgeExistsSql:="select judgeCurrentYearExists("+paramStr+") as id;"
					id0,errorMessage:=api.ExecFuncForOne(judgeExistsSql,"id")
					judgeExistsSqlSub:="select judgeSubjectPeroidExists("+paramStr+") as id1;"
					idSub,errorMessage:=api.ExecFuncForOne(judgeExistsSqlSub,"id1")
					if id0==""{
						if idSub!=""{
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
					judgeNeedUpdateNextKnotsId,errorMessage:=api.ExecFuncForOne(judgeNeedUpdateNextKnotsSql,"id")
					nextYearKnots:=make(map[string]interface{})
					nextYearKnotsSql:="SELECT CONCAT(DATE_FORMAT('"+asyncObjectMap["account_period_year"].(string)+"','%Y'),'-12-31') AS beginYear;"
					nextYearKnotsResult,errorMessage:=api.ExecFuncForOne(nextYearKnotsSql,"beginYear")

					if asyncObjectMap["account_period_num"].(string)=="12"{
						nextYearKnots=asyncObjectMap
						nextYearKnots["line_number"]=102
						nextYearKnots["summary"]="结转下年"
						nextYearKnots["account_period_year"]=nextYearKnotsResult

						paramsMap["account_period_num"]="12"
						paramsMap["account_period_year"]=nextYearKnotsResult
						paramStr=ConcatObjectProperties(funcParamFields,paramsMap)
						asyncObjectMap=CallFunc(api,calculate_field,calculate_func,paramStr,asyncObjectMap)

						if judgeNeedUpdateNextKnotsId!=""{
							nextYearKnots["id"]=judgeNeedUpdateNextKnotsId
							r,errorMessage:=api.Update(operate_table,judgeNeedUpdateNextKnotsId,nextYearKnots)
							fmt.Printf("r=",r,"errorMessage=",errorMessage)
						}else{
							nextYearKnots["id"]=uuid.NewV4().String()
							r,errorMessage:=api.Create(operate_table,nextYearKnots)
							fmt.Printf("r=",r,"errorMessage=",errorMessage)
						}
					}





				}


			}


			if "ASYNC_BATCH_SAVE_SUBJECT_LEAVE"==operate_type {


				asyncObjectMap=BuildMapFromBody(conditionFiledArr,repeatItem,asyncObjectMap)
				asyncObjectMap=BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

				fmt.Printf("operate_table",operate_table)
				fmt.Printf("calculate_field",calculate_field)
				fmt.Printf("calculate_func",calculate_func)

				var paramStr string
				paramsMap:=make(map[string]interface{})
				// funcParamFields
				if operate_func!="" {

					//如果执行方法不为空 执行配置中方法
					paramsMap = BuildMapFromBody(funcParamFields, repeatItem, paramsMap)
					paramsMap = BuildMapFromBody(funcParamFields, repeatItem, paramsMap)
					//把对象的所有属性的值拼成字符串
					paramsMap["account_period_year"]=repeatAccountYear
					paramsMap["account_period_num"]=repeatAccountPeriodNum
					paramStr = ConcatObjectProperties(funcParamFields, paramsMap)

					// 直接执行func 所有逻辑在func处理
					operate_func_sql := "select " + operate_func + "(" + paramStr + ") as result;"
					result,errorMessage := api.ExecFuncForOne(operate_func_sql, "result")
					fmt.Printf("operate_func_sql-result-error", result,errorMessage)
                    if errorMessage!=nil{
                    	tx.Rollback()
					}
				}


			}
			// ASYNC_BATCH_SAVE_SUBJECT_TOTAL
			if "ASYNC_BATCH_SAVE_SUBJECT_TOTAL"==operate_type{
				asyncObjectMap=BuildMapFromBody(conditionFiledArr,repeatItem,asyncObjectMap)
				asyncObjectMap=BuildMapFromBody(conditionFiledArr1,repeatItem,asyncObjectMap)

				fmt.Printf("operate_table",operate_table)
				fmt.Printf("calculate_field",calculate_field)
				fmt.Printf("calculate_func",calculate_func)

				var paramStr string
				paramsMap:=make(map[string]interface{})
				// funcParamFields
				if operate_func!=""{

					//如果执行方法不为空 执行配置中方法
					paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
					paramsMap=BuildMapFromBody(funcParamFields,repeatItem,paramsMap)
					//把对象的所有属性的值拼成字符串
					paramsMap["account_period_year"]=repeatAccountYear
					paramsMap["account_period_num"]=repeatAccountPeriodNum
					paramStr=ConcatObjectProperties(funcParamFields,paramsMap)


					// 直接执行func 所有逻辑在func处理
					operate_func_sql:="select "+operate_func+"("+paramStr+") as result;"
					result,errorMessage:=api.ExecFuncForOne(operate_func_sql,"result")
					fmt.Printf("operate_func_sql-result",result,errorMessage)



				}


			}
			// ASYNC_BATCH_SAVE_SUBJECT_PRE
			if "ASYNC_BATCH_SAVE_SUBJECT_PRE"==operate_type{
				// 如果上一级科目一样 且不是当前id  需要重新计算上级科目余额
				if isCalPre==false{
					var repeatCalculatePreData []map[string]interface{}
					var repeatCalculatePreData0 []map[string]interface{}
					var repeatCalculatePreData1 []map[string]interface{}
					var repeatCalculatePreData2 []map[string]interface{}
					var repeatCalculatePreData3 []map[string]interface{}
					repeatCalculatePreData=append(repeatCalculatePreData,repeatItem)

					in_subject_key:=repeatItem["subject_key"].(string)
					in_farm_id:=repeatItem["farm_id"].(string)
					obtianPreSubjectSql:="select obtainPreSubjectKey('"+in_subject_key+"','"+in_farm_id+"'"+") as pre_subject_key;"
					pre_subject_key,errorMessage:=api.ExecFuncForOne(obtianPreSubjectSql,"pre_subject_key")

					subjectKeyPreWhereOption := map[string]WhereOperation{}

					subjectKeyPreWhereOption["subject_key_pre"] = WhereOperation{
						Operation: "eq",
						Value:     pre_subject_key,
					}
					subjectKeyPreWhereOption["farm_id"] = WhereOperation{
						Operation: "eq",
						Value:     in_farm_id,
					}
					subjectKeyPreWhereOption["voucher_type"] = WhereOperation{
						Operation: "gt",
						Value:     "0",
					}
					subjectKeyPreWhereOption["subject_key"] = WhereOperation{
						Operation: "neq",
						Value:     pre_subject_key,
					}
					subjectKeyPreWhereOption["id"] = WhereOperation{
						Operation: "neq",
						Value:     id,
					}

					var year string
					if repeatItem["account_period_year"]!=nil{
						year=repeatItem["account_period_year"].(string)[0:4]
					}
					subjectKeyPreWhereOption["account_period_year"] = WhereOperation{
						Operation: "gt",
						Value:    year+"-12-31",
					}


					orders:=make(map[string]string)
					orders["N1account_period_num"]="ASC"
					orders["N2account_period_year"]="ASC"
					orders["N3order_num"]="ASC"
					orders["N4line_number"]="ASC"
					preOption := QueryOption{Wheres: subjectKeyPreWhereOption, Table: "account_voucher_detail_category_merge"}
					preOption.Orders=orders
					repeatCalculatePreData0,errorMessage= api.Select(preOption)
					fmt.Printf("errorMessage=",errorMessage)


					subjectKeyPreWhereOption["account_period_year"] = WhereOperation{
						Operation: "like",
						Value:    year+"%",
					}
					subjectKeyPreWhereOption["account_period_num"] = WhereOperation{
						Operation: "gt",
						Value:     repeatItem["account_period_num"],
					}

					preOption = QueryOption{Wheres: subjectKeyPreWhereOption, Table: "account_voucher_detail_category_merge"}
					preOption.Orders=orders
					repeatCalculatePreData1, errorMessage= api.Select(preOption)
					fmt.Printf("errorMessage=",errorMessage)


					subjectKeyPreWhereOption["account_period_num"] = WhereOperation{
						Operation: "eq",
						Value:     repeatItem["account_period_num"],
					}
					subjectKeyPreWhereOption["order_num"] = WhereOperation{
						Operation: "gt",
						Value:     repeatItem["order_num"],
					}

					preOption = QueryOption{Wheres: subjectKeyPreWhereOption, Table: "account_voucher_detail_category_merge"}
					preOption.Orders=orders
					repeatCalculatePreData2, errorMessage= api.Select(preOption)
					fmt.Printf("errorMessage=",errorMessage)


					subjectKeyPreWhereOption["account_period_year"] = WhereOperation{
						Operation: "eq",
						Value:     repeatItem["account_period_year"],
					}
					subjectKeyPreWhereOption["account_period_num"] = WhereOperation{
						Operation: "eq",
						Value:     repeatItem["account_period_num"],
					}
					subjectKeyPreWhereOption["order_num"] = WhereOperation{
						Operation: "eq",
						Value:     repeatItem["order_num"],
					}

					subjectKeyPreWhereOption["line_number"] = WhereOperation{
						Operation: "gt",
						Value:     repeatItem["line_number"],
					}


					preOption = QueryOption{Wheres: subjectKeyPreWhereOption, Table: "account_voucher_detail_category_merge"}
					preOption.Orders=orders
					repeatCalculatePreData3, errorMessage= api.Select(preOption)
					fmt.Printf("errorMessage=",errorMessage)



					for _,item:=range repeatCalculatePreData3{
						repeatCalculatePreData=append(repeatCalculatePreData,item)
					}
					for _,item:=range repeatCalculatePreData2{
						repeatCalculatePreData=append(repeatCalculatePreData,item)
					}
					for _,item:=range repeatCalculatePreData1{
						repeatCalculatePreData=append(repeatCalculatePreData,item)
					}
					for _,item:=range repeatCalculatePreData0{
						repeatCalculatePreData=append(repeatCalculatePreData,item)
					}
					for _,item:=range repeatCalculatePreData{
						if item["id"]!=nil && calpreMap[item["id"].(string)]!=item["id"].(string){
							CalculatePre(api,item,funcParamFields,pre_subject_key,operate_func)
							calpreMap[item["id"].(string)]=item["id"].(string)
						}

					}

					isCalPre=true
					delete(asyncObjectMap,"subject_key_pre")
				}

			}

		}
	}
	fmt.Printf("async-task-end=",time.Now().Format("2006-01-02 15:04:05"))
	// 向管道传值
	c <- index
}