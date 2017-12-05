package swagger

import (
	"github.com/go-openapi/spec"
	"fmt"
	"github.com/shiyongabc/go-mysql-api/server/key"
	. "github.com/shiyongabc/go-mysql-api/types"
)

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
func NewQueryParametersForMySQLAPI() (ps []spec.Parameter) {
	ps=append(NewQueryParametersForCustomPaging(),NewQueryParametersForFilter()...)
	ps=append(ps,NewQueryParametersForOrder()...)
	ps=append(ps,NewQueryParametersForOutputDields()...)
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
		NewQueryArrayParameter(key.KEY_QUERY_WHERE, "指定一个或多个字段筛选 如:\"表名.字段名\".\\[eq,neq,is,isNot,in,notIn,like\\](字段值)", "string", false),
	}
	return
}
func NewQueryParametersForOrder() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryArrayParameter(key.KEY_QUERY_ORDER, "指定一个或多个字段排序 如:\"表名.字段名\"(排序值(默认是升序，desc/asc))", "string", false),
	}
	return
}
func NewQueryParametersForOutputDields() (ps []spec.Parameter) {
	ps = []spec.Parameter{
		NewQueryArrayParameter(key.KEY_QUERY_FIELDS, "指定输出一个或多个字段", "string", false),
		NewQueryArrayParameter(key.KEY_QUERY_LINK, "以单一主键内联的一个或多个表名", "string", false),
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
