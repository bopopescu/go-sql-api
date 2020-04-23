package swagger

import (
	"github.com/go-openapi/spec"
	"fmt"
	. "github.com/shiyongabc/go-sql-api/types"
)

func SwaggerPathsFromDatabaseMetadata(meta *DataBaseMetadata) (paths map[string]spec.PathItem) {
	paths = make(map[string]spec.PathItem)
	clearCachePath := spec.PathItem{}
	batchRelatedPath := spec.PathItem{}
	deleteRelatedPath := spec.PathItem{}
	patchRelatedPath := spec.PathItem{}
	metadataPath := spec.PathItem{}
	patchAsyncPath := spec.PathItem{}
	patchAsyncBatchPath := spec.PathItem{}
	flushTableBatchPath:=spec.PathItem{}

	patchImportPath := spec.PathItem{}
	patchFuncPath := spec.PathItem{}


	patchRemotePath := spec.PathItem{}
	databaseName:=meta.DatabaseName
	patchRemotePath.Get=NewOperation(
		"request remote api",
		fmt.Sprintf("执行远程api"),
		fmt.Sprintf("执行远程api"),

		//[]spec.Parameter{ImportParameters(),ImportTemplateParameters()...},//append([]spec.Parameter{ImportTemplateParameter()},spec.FileParam("file")...),// NewQueryParametersForOutputDields()
		append([]spec.Parameter{},ExecRemoteParameters()...),// ImportTemplateParameters()

		fmt.Sprintf("执行远程api"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
			},
		},
	)
	paths["/api/"+databaseName+"/remote/"]=patchRemotePath


	patchFuncPath.Post=NewOperation(
		"exec func",
		fmt.Sprintf("执行数据库func"),
		fmt.Sprintf("执行数据库func"),

		//[]spec.Parameter{ImportParameters(),ImportTemplateParameters()...},//append([]spec.Parameter{ImportTemplateParameter()},spec.FileParam("file")...),// NewQueryParametersForOutputDields()
		append([]spec.Parameter{},ExecFuncParameters()...),// ImportTemplateParameters()

		fmt.Sprintf("执行数据库func"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
			},
		},
	)
	paths["/api/"+databaseName+"/func/"]=patchFuncPath


	patchImportPath.Post=NewOperation(
		"import data to template",
		fmt.Sprintf("按模板导入数据(导入的excel必须是xlsx后缀的文件)"),
		fmt.Sprintf("按模板导入数据(导入的excel必须是xlsx后缀的文件)"),

		//[]spec.Parameter{ImportParameters(),ImportTemplateParameters()...},//append([]spec.Parameter{ImportTemplateParameter()},spec.FileParam("file")...),// NewQueryParametersForOutputDields()
		append([]spec.Parameter{ImportParameters()},ImportTemplateParameters()...),// ImportTemplateParameters()

		fmt.Sprintf("按模板导入数据"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
			},
		},
	)
	paths["/api/"+databaseName+"/import/"]=patchImportPath



	patchAsyncPath.Get=NewOperation(
		"exec async task",
		fmt.Sprintf("执行指定异步任务key"),
		fmt.Sprintf("执行指定异步任务key"),

		append([]spec.Parameter{NewPathWhereParameter()},NewQueryParametersForAsync()...),
		fmt.Sprintf("执行指定异步任务key"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"string"},
			},
		},
	)
	paths["/api/"+databaseName+"/async/"]=patchAsyncPath
// patchAsyncBatchPath

	patchAsyncBatchPath.Get=NewOperation(
		"exec batch async task",
		fmt.Sprintf("批量执行指定异步任务key"),
		fmt.Sprintf("批量执行指定异步任务key"),

		append([]spec.Parameter{},NewQueryParametersForAsync()...),
		fmt.Sprintf("执行指定异步任务key"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"string"},
			},
		},
	)
	paths["/api/"+databaseName+"/async/batch"]=patchAsyncBatchPath


	flushTableBatchPath.Get=NewOperation(
		"exec flush table structure",
		fmt.Sprintf("刷新表结构"),
		fmt.Sprintf("刷新表结构"),

		append([]spec.Parameter{}),
		fmt.Sprintf("刷新表结构"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"string"},
			},
		},
	)
	paths["/api/"+databaseName+"/table/flush"]=flushTableBatchPath


	clearCachePath.Get=NewOperation(
		"clear-cache",
		fmt.Sprintf("清除指定key的缓存"),
		fmt.Sprintf("清除指定key的缓存"),
		NewQueryParametersForClearCache(),


		fmt.Sprintf("清除指定key的缓存"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"string"},
			},
		},
	)
	paths["/api/"+databaseName+"/clear/cache/"]=clearCachePath



	deleteRelatedPath.Delete=NewOperation(
		"relate record(关联记录)",
		fmt.Sprintf("删除关联记录数据"),
		fmt.Sprintf("删除关联记录数据"),
		[]spec.Parameter{GetParametersFromRelatedRecordDelete()},


		fmt.Sprintf("关联表同时删除关联数据"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
			},
		},
	)
	paths["/api/"+databaseName+"/related/delete/"]=deleteRelatedPath


	batchRelatedPath.Post=NewOperation(
		"relate record(关联记录)",
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
	paths["/api/"+databaseName+"/related/batch/"]=batchRelatedPath

	patchRelatedPath.Put=NewOperation(
		"relate record(关联记录)",
		fmt.Sprintf("修改关联记录数据"),
		fmt.Sprintf("修改关联记录数据"),
		[]spec.Parameter{GetParametersFromRelatedRecord()},


		fmt.Sprintf("关联表同时同时数据"),
		&spec.Schema{
			SchemaProps: spec.SchemaProps{
				Type: spec.StringOrArray{"object"},
			},
		},
	)
	paths["/api/"+databaseName+"/related/record/"]=patchRelatedPath






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
	paths["/api/"+databaseName+"/metadata/"]=metadataPath

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
	paths["/api/"+databaseName+"/echo/"]=echoPath
	for _, t := range meta.Tables {
		AppendPathsFor(t, paths,meta)
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
func NewGetOperationForPost(tName string) (op *spec.Operation){
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

func AppendPathsFor(meta *TableMetadata, paths map[string]spec.PathItem,metaBase *DataBaseMetadata) () {
	tName := meta.TableName
	isView := meta.TableType == "VIEW"
	withoutIDPathItem := spec.PathItem{}
	withoutIDPathItem1 := spec.PathItem{}
	withoutIDPathPatchItem := spec.PathItem{}
	withIDPathItem := spec.PathItem{}
	withoutIDBatchPathItem := spec.PathItem{}
	withoutIDBatchPutPathItem := spec.PathItem{}
	databaseName:=metaBase.DatabaseName
	apiNoIDPath := fmt.Sprintf("/api/"+databaseName+"/%s", tName)
	apiNoIDPath1 := fmt.Sprintf("/api/"+databaseName+"/%s/query/", tName)
	if !isView {
		// /api/"+databaseName+"/:table group
		withoutIDPathItem.Get =NewGetOperation(tName)
		withoutIDPathItem1.Post =NewGetOperation(tName)
		paths[apiNoIDPath1] = withoutIDPathItem1
		// /api/"+databaseName+"/:table group
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
			// /api/"+databaseName+"/:table/:id group
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
			withIDPathItem.Put = NewOperation(
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
			apiIDPath := fmt.Sprintf("/api/"+databaseName+"/%s/{%s}", tName,columnNames(meta.GetPrimaryColumns()),)
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

		apiBatchPath := fmt.Sprintf("/api/"+databaseName+"/%s/batch/", tName)


		paths[apiBatchPath] = withoutIDBatchPathItem
		// withoutIDBatchPutPathItem
		withoutIDBatchPutPathItem.Put = NewOperation(
			tName,
			fmt.Sprintf("在%s表里,批量更新记录(根据主键id更新,如果没有主键值则添加)", tName),
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

		apiBatchPutPath := fmt.Sprintf("/api/"+databaseName+"/%s/batch/", tName)


		paths[apiBatchPutPath] = withoutIDBatchPutPathItem

		withoutIDPathPatchItem.Patch = NewOperation(
			tName,
			fmt.Sprintf("从%s表里,更新满足条件的记录", tName),
			fmt.Sprintf("%s表的主键%s", tName,columnNames(meta.GetPrimaryColumns())),
			append([]spec.Parameter{NewPathWhereParameter()},NewParamForDefinition(tName)),
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
		apiBatchPathPatch := fmt.Sprintf("/api/"+databaseName+"/%s/where/", tName,)
		//apiBatchPath := fmt.Sprintf("/api/"+databaseName+"/%s/batch/", tName)


		paths[apiBatchPathPatch] = withoutIDPathPatchItem

		withoutIDPathPatchItem.Put = NewOperation(
			tName,
			fmt.Sprintf("从%s表里,更新满足条件的记录", tName),
			fmt.Sprintf("%s表的主键%s", tName,columnNames(meta.GetPrimaryColumns())),
			append([]spec.Parameter{NewPathWhereParameter()},NewParamForDefinition(tName)),
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
		apipPutPathPatch := fmt.Sprintf("/api/"+databaseName+"/%s/where/", tName,)
		//apiBatchPath := fmt.Sprintf("/api/"+databaseName+"/%s/batch/", tName)


		paths[apipPutPathPatch] = withoutIDPathPatchItem
	}else {
		// /api/"+databaseName+"/:table group
		withoutIDPathItem.Get =NewGetOperation(tName)
		withoutIDPathItem1.Post =NewGetOperation(tName)

		paths[apiNoIDPath] = withoutIDPathItem
		paths[apiNoIDPath1] = withoutIDPathItem1
	}
	return
}


