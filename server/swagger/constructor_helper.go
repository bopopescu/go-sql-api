package swagger

import (
	"github.com/go-openapi/spec"
	"fmt"
	"github.com/shiyongabc/go-sql-api/server/key"
	. "github.com/shiyongabc/go-sql-api/types"
	//"github.com/shiyongabc/go-mysql-api/adapter/mysql"
)

type test struct {
	id string `json:"id"`
	name string `json:"name"`
}
func NewRefSchema(refDefinationName, reftype string) (s spec.Schema) {
	s = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: spec.StringOrArray{reftype},
			Items: &spec.SchemaOrArray{
				&spec.Schema{
					spec.VendorExtensible{},
					spec.SchemaProps{
						Ref: getTableSwaggerRef(refDefinationName),
					},
					spec.SwaggerSchemaProps{},
					nil,
				},
				nil,
			},
		},
	}
	return
}

func NewField(sName, sType string, iExample interface{}) (s spec.Schema) {
	s = spec.Schema{
		spec.VendorExtensible{},
		spec.SchemaProps{
			Type:  spec.StringOrArray{sType},
			Title: sName,
		},
		spec.SwaggerSchemaProps{
			Example: iExample,
		},
		nil,
	}
	return
}

func NewCUDOperationReturnMessage() (s spec.Schema) {
	s = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: spec.StringOrArray{"object"},
			Properties: map[string]spec.Schema{
				"lastInsertID":  NewField("lastInsertID", "integer", 0),
				"rowesAffected": NewField("rowesAffected", "integer", 1),
			},
		},
	}
	return
}

func NewCUDOperationReturnArrayMessage() (s spec.Schema) {
	s = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: spec.StringOrArray{"array"},
			Items: &spec.SchemaOrArray{
				Schema: &spec.Schema{
					SchemaProps: spec.SchemaProps{
						Properties: map[string]spec.Schema{
							"lastInsertID":  NewField("lastInsertID", "integer", 0),
							"rowesAffected": NewField("rowesAffected", "integer", 1),
						},
					},
				},
			},
		},
	}
	return
}

func NewDefinitionMessageWrap(definitionName string, data spec.Schema) (sWrap *spec.Schema) {

	sWrap = &spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: spec.StringOrArray{"object"},
			Properties: map[string]spec.Schema{
				"status":  NewField("status", "integer", 200),
				"message": NewField("message", "string", nil),
				"data":    data,
			},
		},
		SwaggerSchemaProps: spec.SwaggerSchemaProps{},
	}
	return
}

func NewSwaggerInfo(meta *DataBaseMetadata, version string) (info *spec.Info) {
	info = &spec.Info{spec.VendorExtensible{}, spec.InfoProps{
		Title:       fmt.Sprintf("Database %s RESTful API", meta.DatabaseName),
		Version:     version,
		Description: "To the time to life, rather than to life in time.",
	}}
	return
}

func GetParametersFromDbMetadata(meta *DataBaseMetadata) (params map[string]spec.Parameter) {
	params = make(map[string]spec.Parameter)
	for _, t := range meta.Tables {
		for _, col := range t.Columns {
			params[col.ColumnName] = spec.Parameter{
				ParamProps: spec.ParamProps{
					In:          "body",
					Description: col.Comment,
					Name:        col.ColumnName,
					Required:    col.NullAble == "true",
				},
			}
		}
	}
	return
}
func GetParametersFromRelatedRecordDelete() (p spec.Parameter) {

	schema := spec.Schema{}
	schema.Type = spec.StringOrArray{"object"}
	schema.Title = "relatedRecord"
	schema.Description = "关联记录"
	schemaProps:=spec.SchemaProps{}
	schemaProps.Type=spec.StringOrArray{"object"}
	schemaProps.Title="relatedRecord"
	schemaProps.Description="关联记录"
	schemaProps.Properties=map[string]spec.Schema{}

	schemaProps.Properties["masterTableName"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "主表名",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	schemaProps.Properties["masterTableInfo"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "主表数据信息(字符串对象)",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}


	schemaProps.Properties["slaveTableName"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "从表名",
			//Title:       col.ColumnName,
			Default:     "",

		},
	}
	schemaProps.Properties["isRetainMasterInfo"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "是否保留主表信息(1 是  0 不保留)",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}

	schema.SchemaProps=schemaProps
	p = spec.Parameter{
		ParamProps: spec.ParamProps{
			In:     "body",
			Name:   "body",
			Required:true,
			Description:fmt.Sprintf("需要提交的关联记录对象", "relatedRecord"),
			Schema: &schema,
		},

	}

	return
}

func GetParametersFromRelatedRecord() (p spec.Parameter) {

	schema := spec.Schema{}
	schema.Type = spec.StringOrArray{"object"}
	schema.Title = "relatedRecord"
	schema.Description = "关联记录"
	schemaProps:=spec.SchemaProps{}
	schemaProps.Type=spec.StringOrArray{"object"}
	schemaProps.Title="relatedRecord"
	schemaProps.Description="关联记录"
	schemaProps.Properties=map[string]spec.Schema{}

	schemaProps.Properties["masterTableName"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "主表名",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	schemaProps.Properties["masterTableInfo"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "主表数据信息(字符串对象)",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}


	schemaProps.Properties["slaveTableName"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "从表名",
			//Title:       col.ColumnName,
			Default:     "",

		},
	}
	schemaProps.Properties["slaveTableInfo"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "从表数据信息(字符串对象)",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}

	schema.SchemaProps=schemaProps
	p = spec.Parameter{
		ParamProps: spec.ParamProps{
			In:     "body",
			Name:   "body",
			Required:true,
			Description:fmt.Sprintf("需要提交的关联记录对象", "relatedRecord"),
			Schema: &schema,
		},

	}

	return
}

func ImportParameters() (p spec.Parameter) {

	schema := spec.Schema{}
	schemaProps:=spec.SchemaProps{}
	schemaProps.Properties=map[string]spec.Schema{}
	//schemaProps.Properties[key.IMPORT_TEMPLATE_KEY] = spec.Schema{
	//	SchemaProps: spec.SchemaProps{
	//		Type:        spec.StringOrArray{"string"},
	//		Description: "导入模板key",
	//		//Title:       col.ColumnName,
	//		Default:     "",
	//	},
	//}




	schemaProps.Properties["file"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:         spec.StringOrArray{"string"},
			Description: "字段类型",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}

	schema.SchemaProps=schemaProps
	p = spec.Parameter{
		ParamProps: spec.ParamProps{
			In:     "formData",
			Name:   "body",
			Required:true,
			Description:"导入模板对象",
			Schema: &schema,
		},
//  ParamProps: ParamProps{Name: name, In: "formData"}, SimpleSchema: SimpleSchema{Type: "file"}
	}
p=*spec.FileParam("file")
	return
}


func GetParametersFromCreateTableColumn() (p spec.Parameter) {

	schema := spec.Schema{}
	schemaProps:=spec.SchemaProps{}
	schemaProps.Properties=map[string]spec.Schema{}
	schemaProps.Properties["tableName"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "表的英文名字",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	schemaProps.Properties["columnName"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "列名",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	schemaProps.Properties["isFirst"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "是否是第一列",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	// beforeColumnName
	schemaProps.Properties["afterColumnName"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "指定在哪一列的后面",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}

	schemaProps.Properties["columnType"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "字段类型",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	schemaProps.Properties["defaultValue"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "默认值",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	// columnDes
	schemaProps.Properties["columnDes"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "劣描述",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	schema.SchemaProps=schemaProps
	p = spec.Parameter{
		ParamProps: spec.ParamProps{
			In:     "body",
			Name:   "body",
			Required:true,
			Description:fmt.Sprintf("字段属性", "relatedRecord"),
			Schema: &schema,
		},

	}

	return
}


func GetParametersFromCreateTableStructure() (p spec.Parameter) {

	schema := spec.Schema{}
	//schema.Type = spec.StringOrArray{"object"}
	//schema.Title = "relatedRecord"
	//schema.Description = "关联记录"
	schemaProps:=spec.SchemaProps{}
	//schemaProps.Type=spec.StringOrArray{"object"}
	//schemaProps.Title="relatedRecord"
	//schemaProps.Description="关联记录"
	schemaProps.Properties=map[string]spec.Schema{}

	schemaProps.Properties["tableName"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "表的英文名字",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	schemaProps.Properties["tableNameDesc"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "表的中文名字",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	schemaProps.Properties["isReport"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "是否是报表(1 是 0 不是)",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	schemaProps.Properties["ownerOrgId"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "报表的拥有者(为空时表示默认是模板报表  不为空则为自定客服的自定义报表)",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}
	schemaProps.Properties["tableFields"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type:        spec.StringOrArray{"string"},
			Description: "表的字段（字符串数组）",
			//Title:       col.ColumnName,
			Default:     "",
		},
	}

	schema.SchemaProps=schemaProps
	p = spec.Parameter{
		ParamProps: spec.ParamProps{
			In:     "body",
			Name:   "body",
			Required:true,
			Description:fmt.Sprintf("需要提交的表结构对象", "relatedRecord"),
			Schema: &schema,
		},

	}

	return
}

func NewQueryParametersForMySQLAPI() (ps []spec.Parameter) {
	ps=append(NewQueryParametersForCustomPaging(),NewQueryParametersForFilter()...)
	ps=append(ps,NewQueryParametersForOrder()...)
	ps=append(ps,NewQueryParametersForGroup()...)
	ps=append(ps,NewQueryParametersForOutputDields()...)
	ps=append(ps,NewQueryParametersForSub()...)
	return
}
func NewQueryParametersForQueryTableStructure() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryParameter(key.TABLE_NAME, "tableName", "string", true),
	}
	return
}

func NewQueryParametersForClearCache() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryParameter(key.KEY_CACHE, "缓存key", "string", true),
	}
	return
}
func ImportTemplateParameters() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryParameter(key.IMPORT_TEMPLATE_KEY, "导入模板key", "string", true),
		NewQueryParameter(key.KEY_QUERY_WHERE, "指定一个或多个字段筛选 如:表名.字段名.[eq,neq,is,isNot,in,notIn,like,lt,gt](字段值)(多个条件用&拼接)", "string", false),
	}
	return
}
func ExecFuncParameters() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryParameter(key.FUNC_KEY, "funcKey", "string", true),
	}
	return
}
func ExecRemoteParameters() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryParameter(key.AUTHORIZATION_KEY, "authorization", "string", true),
	}
	return
}
func NewQueryParametersForAsync() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryParameter(key.ASYNC_KEY, "asyncKey", "string", true),
	}
	return
}
func NewQueryParametersForCustomPaging() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryParameter(key.KEY_QUERY_PAGEINDEX, "分页页码(从1开始编号)", "integer", false),
		NewQueryParameter(key.KEY_QUERY_PAGESIZE, "分页大小", "integer", false),
	}
	return
}
func NewQueryParametersForFilter() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryParameter(key.KEY_QUERY_SEARCH, "全表查找字符串", "string", false),
		NewQueryParameter(key.GROUP_FUNC, "聚合函数(SUM(tableName.column)),如果有多个引号里面的内容用|代替, 字段分组字段 返回字段（即就是分组的字段）", "string", false),
		NewQueryArrayParameter(key.KEY_QUERY_WHERE, "多条件and链接查询：指定一个或多个字段筛选 如:\"表名.字段名\".\\[eq,neq,is,isNot,in,notIn,like,lt,gt,lte,gte\\](字段值)", "string", false),
		NewQueryArrayParameter(key.KEY_QUERY_OR_WHERE, "多条件or链接查询：指定一个或多个字段 如:\"表名.字段名\".\\[eq,neq,is,isNot,in,notIn,like,lt,gt,lte,gte\\](字段值)", "string", false),
		NewQueryArrayParameter(key.KEY_QUERY_OR_WHERE_AND, "or嵌套and条件查询(4个where)：指定一个或多个字段 如:\"表名.字段名\".\\[eq,neq,is,isNot,in,notIn,like,lt,gt,lte,gte\\](字段值)", "string", false),

	}
	return
}


func NewQueryParametersForOrder() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryArrayParameter(key.KEY_QUERY_ORDER, "指定一个或多个字段排序 如:\"表名.字段名\"(排序值(默认是升序，desc/asc))", "string", false),
	}
	return
}
// GroupFields
func NewQueryParametersForGroup() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryArrayParameter(key.GROUP_BY, "指定一个或多个字段分组", "string", false),
	}
	return
}


func NewQueryParametersForOutputDields() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryArrayParameter(key.KEY_QUERY_FIELDS, "指定输出一个或多个字段", "string", false),
		NewQueryParameter(key.KEY_QUERY_FIELDS_TYPE, "0 默认查询该表所有字段 1 自定义报表虚拟子表的所有字段", "string", false),
		NewQueryArrayParameter(key.KEY_QUERY_LINK, "以单一主键内联的一个或多个表名", "string", false),
	}
	return
}
func NewQueryParametersForSub() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryParameter(key.SUB_KEY, "指定子查询key(subTableName.key)", "string", false),
		NewQueryArrayParameter(key.SUB_KEY_QUERY_FIELDS, "指定查询子查询字段", "string", false),

	}
	return
}
func NewQueryArrayParameter(paramName, paramDescription, paramType string, required bool) (p spec.Parameter) {
	p = spec.Parameter{
		SimpleSchema: spec.SimpleSchema{
			Type: "array",
			CollectionFormat: "multi",
			Items:&spec.Items{
				SimpleSchema:spec.SimpleSchema{
					Type: paramType,
				},
			},
		},
		ParamProps: spec.ParamProps{
			In:          "query",
			Name:        paramName,
			Required:    required,
			Description: paramDescription,
		},
	}
	return
}

func NewQueryParameter(paramName, paramDescription, paramType string, required bool) (p spec.Parameter) {
	p = spec.Parameter{
		SimpleSchema: spec.SimpleSchema{
			Type: paramType,
		},
		ParamProps: spec.ParamProps{
			In:          "query",
			Name:        paramName,
			Required:    required,
			Description: paramDescription,
		},
	}
	return
}
func ImportTemplateParameter() (p spec.Parameter) {
	p = spec.Parameter{
		SimpleSchema: spec.SimpleSchema{
			Type: "string",
		},
		ParamProps: spec.ParamProps{
			In:          "query",
			Name:        key.IMPORT_TEMPLATE_KEY,
			Required:    true,
			Description: "导入模板key",
		},
	}
	return
}

func NewPathIDParameter(tMeta *TableMetadata) (p spec.Parameter) {
	p = spec.Parameter{
		SimpleSchema: spec.SimpleSchema{
			Type: "string",
		},
		ParamProps: spec.ParamProps{
			In:          "path",
			Name:        columnNames(tMeta.GetPrimaryColumns()),
			Required:    true,
			Description: fmt.Sprintf("/%s", columnNames(tMeta.GetPrimaryColumns()) ),
		},
	}
	return
}
func NewPathWhereParameter() (p spec.Parameter) {
	p = spec.Parameter{
		SimpleSchema: spec.SimpleSchema{
			Type: "string",
		},
		ParamProps: spec.ParamProps{
			In:          "query",
			Name:        "where",
			Required:    true,
			Description: "指定一个或多个字段筛选 如:表名.字段名.[eq,neq,is,isNot,in,notIn,like,lt,gt](字段值)(多个条件用&拼接)",
		},
	}
	return
}

func columnNames(primaryColumns []*ColumnMetadata) (names string){
	for i,v := range primaryColumns{
		if(i>0){
			names=names+","+v.ColumnName
		}else {
			names=""+v.ColumnName
		}
	}
	return
}


func NewParamForArrayDefinition(tName string) (p spec.Parameter) {
	s := NewRefSchema(tName, "array")
	p = spec.Parameter{
		ParamProps: spec.ParamProps{
			In:     "body",
			Name:   "body",
			Required:true,
			Description:fmt.Sprintf("需要提交的%s对象数组", tName),
			Schema: &s,
		},
	}
	return
}

func NewParamForDefinition(tName string) (p spec.Parameter) {
	p = spec.Parameter{
		ParamProps: spec.ParamProps{
			In:     "body",
			Name:   "body",
			Required:true,
			Description:fmt.Sprintf("需要提交的%s对象", tName),
			Schema: getTableSwaggerRefSchema(tName),
		},
	}
	return
}

func NewOperation(tName,summary, opDescribetion string, params []spec.Parameter,responseDescription string, respSchema *spec.Schema) (op *spec.Operation) {
	op = &spec.Operation{
		spec.VendorExtensible{}, spec.OperationProps{
			Summary:summary,
			Description: opDescribetion,
			//Produces:[]string{"application/json","application/octet-stream"},
			Tags:        []string{tName},
			Parameters:  params,
			Responses: &spec.Responses{
				spec.VendorExtensible{},
				spec.ResponsesProps{
					&spec.Response{
						ResponseProps:spec.ResponseProps{
							Description:"错误消息",
							Schema: &spec.Schema{
								SchemaProps:spec.SchemaProps{
									Ref:getTableSwaggerRef("error_message"),
								},
							},
						},
					},
					map[int]spec.Response{
						200: {
							ResponseProps: spec.ResponseProps{
								Description: responseDescription,
								Schema: respSchema,
							},
						},
						401:{
							ResponseProps: spec.ResponseProps{
								Description: "未认证",
							},
						},
						403:{
							ResponseProps: spec.ResponseProps{
								Description: "未授权",
							},
						},
					},
				},
			},
		},
	}
	return
}

func NewTag(t string) (tag spec.Tag) {
	tag = spec.Tag{TagProps: spec.TagProps{Name: t}}
	return
}

func NewTagsForOne(t string) (tags []spec.Tag) {
	tags = []spec.Tag{NewTag(t)}
	return
}
