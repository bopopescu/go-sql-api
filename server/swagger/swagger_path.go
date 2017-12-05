package swagger

import (
	"github.com/go-openapi/spec"
	"fmt"
	. "github.com/shiyongabc/go-mysql-api/types"
)

func SwaggerPathsFromDatabaseMetadata(meta *DataBaseMetadata) (paths map[string]spec.PathItem) {
	paths = make(map[string]spec.PathItem)
	batchRelatedPath := spec.PathItem{}
	metadataPath := spec.PathItem{}

	batchRelatedPath.Post=NewOperation(
		"",
		fmt.Sprintf("添加关联记录数据"),
		fmt.Sprintf("添加关联记录数据"),
		[]spec.Parameter{GetParametersFromRelatedRecord()},


		fmt.Sprintf("关联表同时插入数据"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
			},
		},
	)
	paths["/api/related/batch/"]=batchRelatedPath


	metadataPath.Head=NewOperation(
		"metadata",
		fmt.Sprintf("从DB加载最新的元数据"),
		fmt.Sprintf("变更库后,最长5分钟才能自动加载新的元数据,如需立即生效,则使用当前api"),
		[]spec.Parameter{},
		fmt.Sprintf("总是返回1"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"integer"},
			},
			SwaggerSchemaProps: spec.SwaggerSchemaProps{
				Example: 1,
			},
		},
	)
	metadataPath.Get=NewOperation(
		"metadata",
		fmt.Sprintf("返回当前加载的元数据"),
		fmt.Sprintf("元数据(注意:每5分钟自动加载新的元数据)"),
		[]spec.Parameter{},
		fmt.Sprintf("元数据"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
			},
		},
	)
	paths["/api/metadata/"]=metadataPath

	echoPath := spec.PathItem{}
	echoPath.Post=NewOperation(
		"metadata",
		fmt.Sprintf("参数和心跳检查"),
		fmt.Sprintf("当前api用于确定参数是否到达和服务是否正常"),
		[]spec.Parameter{{
			ParamProps: spec.ParamProps{
				In:     "body",
				Name:   "body",
				Description:fmt.Sprintf("参数对象"),
				Schema: &spec.Schema{
					SchemaProps: spec.SchemaProps{
						Type: spec.StringOrArray{"object"},
					},
				},
			},
		}},
		fmt.Sprintf("总是原样返回请求参数"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
			},
		},
	)
	paths["/api/echo/"]=echoPath
	for _, t := range meta.Tables {
		AppendPathsFor(t, paths)
	}
	return
}

func NewGetOperation(tName string) (op *spec.Operation){
	op=NewOperation(
		tName,
		fmt.Sprintf("从%s表里,查询记录", tName),
		fmt.Sprintf("数组对象返回(未指定index),或分页返回(指定index)"),
		NewQueryParametersForMySQLAPI(),
		fmt.Sprintf("分页返回数据(注意:当未指定index时,直接返回[]数组对象,无分页指示对象包裹)"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
				Properties: map[string]spec.Schema{
					"pageIndex":{
						SchemaProps: spec.SchemaProps{
							Type: spec.StringOrArray{"integer"},
						},
						SwaggerSchemaProps: spec.SwaggerSchemaProps{
							Example: 1,
						},
					},
					"pageSize": {
						SchemaProps: spec.SchemaProps{
							Type: spec.StringOrArray{"integer"},
						},
						SwaggerSchemaProps: spec.SwaggerSchemaProps{
							Example: 10,
						},
					},
					"totalPages": {
						SchemaProps: spec.SchemaProps{
							Type: spec.StringOrArray{"integer"},
						},
						SwaggerSchemaProps: spec.SwaggerSchemaProps{
							Example: 1,
						},
					},
					"totalCount": {
						SchemaProps: spec.SchemaProps{
							Type: spec.StringOrArray{"integer"},
						},
						SwaggerSchemaProps: spec.SwaggerSchemaProps{
							Example: 1,
						},
					},
					"data":    spec.Schema{
						SchemaProps: spec.SchemaProps{
							Type: spec.StringOrArray{"array"},
							Items:&spec.SchemaOrArray{
								Schema:&spec.Schema{
									SchemaProps: spec.SchemaProps{
										Ref: getTableSwaggerRef(tName),
									},
								},
							},
						},
					},
				},
			},
		},
	)
	op.Produces=[]string{"application/json","application/octet-stream"}
	return
}

func AppendPathsFor(meta *TableMetadata, paths map[string]spec.PathItem) () {
	tName := meta.TableName
	isView := meta.TableType == "VIEW"
	withoutIDPathItem := spec.PathItem{}
	withIDPathItem := spec.PathItem{}
	withoutIDBatchPathItem := spec.PathItem{}
	withoutTranslatePathItem := spec.PathItem{}
	apiNoIDPath := fmt.Sprintf("/api/%s", tName)
	if !isView {
		// /api/:table group
		withoutIDPathItem.Get =NewGetOperation(tName)
		// /api/:table group
		withoutIDPathItem.Post = NewOperation(
			tName,
			fmt.Sprintf("在%s表里,插入一条记录", tName),
			"",
			[]spec.Parameter{NewParamForDefinition(tName)},
			fmt.Sprintf("执行成功,返回影响行数(注意:以影响行数为判断成功与否的依据)"),
			&spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: spec.StringOrArray{"integer"},
				},
				SwaggerSchemaProps: spec.SwaggerSchemaProps{
					Example: 1,
				},
			},
		)
		withoutIDPathItem.Delete = NewOperation(
			tName,
			fmt.Sprintf("在%s表里,删除指定条件的记录", tName),
			fmt.Sprintf("为防止误删除,body里必须有条件"),
			[]spec.Parameter{NewParamForDefinition(tName)},
			fmt.Sprintf("执行成功,返回影响行数(注意:以影响行数为判断成功与否的依据)"),
			&spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: spec.StringOrArray{"integer"},
				},
				SwaggerSchemaProps: spec.SwaggerSchemaProps{
					Example: 0,
				},
			},
		)
		paths[apiNoIDPath] = withoutIDPathItem

		if(len(meta.GetPrimaryColumns())>0){
			// /api/:table/:id group
			withIDPathItem.Get = NewOperation(
				tName,
				fmt.Sprintf("从%s表里,查询指定主键的记录", tName),
				fmt.Sprintf("%s表的主键%s", tName,columnNames(meta.GetPrimaryColumns())),
				append([]spec.Parameter{NewPathIDParameter(meta)},NewQueryParametersForOutputDields()...),
				fmt.Sprintf("返回数据"),
				&spec.Schema{
					SchemaProps: spec.SchemaProps{
						Type: spec.StringOrArray{"object"},
						Ref: getTableSwaggerRef(tName),
					},
				},
			)
			withIDPathItem.Patch = NewOperation(
				tName,
				fmt.Sprintf("在%s表里,更新指定主键的记录", tName),
				fmt.Sprintf("%s表的主键%s", tName,columnNames(meta.GetPrimaryColumns())),
				append([]spec.Parameter{NewPathIDParameter(meta)},NewParamForDefinition(tName)),
				fmt.Sprintf("执行成功,返回影响行数(注意:以影响行数为判断成功与否的依据)"),
				&spec.Schema{
					SchemaProps: spec.SchemaProps{
						Type: spec.StringOrArray{"integer"},
					},
					SwaggerSchemaProps: spec.SwaggerSchemaProps{
						Example: 1,
					},
				},
			)
			withIDPathItem.Delete = NewOperation(
				tName,
				fmt.Sprintf("在%s表里,删除指定主键的记录", tName),
				fmt.Sprintf("%s表的主键%s", tName,columnNames(meta.GetPrimaryColumns())),
				append([]spec.Parameter{}, NewPathIDParameter(meta)),
				fmt.Sprintf("执行成功,返回影响行数(注意:以影响行数为判断成功与否的依据)"),
				&spec.Schema{
					SchemaProps: spec.SchemaProps{
						Type: spec.StringOrArray{"integer"},
					},
					SwaggerSchemaProps: spec.SwaggerSchemaProps{
						Example: 1,
					},
				},
			)
			apiIDPath := fmt.Sprintf("/api/%s/{%s}", tName,columnNames(meta.GetPrimaryColumns()),)
			paths[apiIDPath] = withIDPathItem
		}
		// Batch group
		withoutIDBatchPathItem.Post = NewOperation(
			tName,
			fmt.Sprintf("在%s表里,批量插入记录", tName),
			"",
			[]spec.Parameter{NewParamForArrayDefinition(tName)},
			fmt.Sprintf("执行成功,返回影响行数(注意:以影响行数为判断成功与否的依据)"),
			&spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: spec.StringOrArray{"integer"},
				},
				SwaggerSchemaProps: spec.SwaggerSchemaProps{
					Example: 0,
				},
			},
		)
		apiBatchPath := fmt.Sprintf("/api/%s/batch/", tName)

		paths[apiBatchPath] = withoutIDBatchPathItem
		// withoutTranslatePathItem

		withoutTranslatePathItem.Post = NewOperation(
			tName,
			fmt.Sprintf("在%s表里,批量插入记录", tName),
			"",
			[]spec.Parameter{NewParamForArrayDefinition(tName)},
			fmt.Sprintf("执行成功,返回影响行数(注意:以影响行数为判断成功与否的依据)"),
			&spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type: spec.StringOrArray{"integer"},
				},
				SwaggerSchemaProps: spec.SwaggerSchemaProps{
					Example: 0,
				},
			},
		)
		apiTranslatePath := fmt.Sprintf("/api/%s/translate/", tName)

		paths[apiTranslatePath] = withoutTranslatePathItem
	}else {
		// /api/:table group
		withoutIDPathItem.Get =NewGetOperation(tName)
		paths[apiNoIDPath] = withoutIDPathItem
	}
	return
}


