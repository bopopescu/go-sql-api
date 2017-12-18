package swagger

import (
	"github.com/go-openapi/spec"
	"github.com/go-openapi/jsonreference"
	_ "github.com/shiyongabc/go-mysql-api/adapter"
	types    "github.com/shiyongabc/go-mysql-api/types"
	"fmt"


)

func SwaggerDefinationsFromDabaseMetadata(dbMeta *types.DataBaseMetadata) (definations spec.Definitions) {
	definations = spec.Definitions{}
	for _, t := range dbMeta.Tables {
		schema := spec.Schema{}
		schema.Type = spec.StringOrArray{"object"}
		schema.Title = t.TableName
		schema.Description = t.Comment
		schema.SchemaProps = SchemaPropsFromTbmeta(t)
		definations[t.TableName] = schema
	}
	//ErrorMessage
	schema := spec.Schema{}
	schema.Type = spec.StringOrArray{"object"}
	schema.Title = "错误消息"
	schema.Description = "意外的错误时的消息"
	schema.SchemaProps = spec.SchemaProps{
		Required:[]string{"error"},
		Properties: map[string]spec.Schema{
			"error":spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type:        spec.StringOrArray{"string"},
					Description: "消息标识",
				},
			},
			"error_description":spec.Schema{
				SchemaProps: spec.SchemaProps{
					Type:        spec.StringOrArray{"string"},
					Description: "消息描述",
				},
			},
		},
	}
	definations["error_message"]=schema
	return
}

func getTableSwaggerRef(t string) (ref spec.Ref) {
	ref = spec.Ref{}
	ref.Ref, _ = jsonreference.New(fmt.Sprintf("#/definitions/%s", t))
	return
}

func getTableSwaggerRefAble(t string) (refable spec.Refable) {
	refable = spec.Refable{getTableSwaggerRef(t)}
	return
}

//type MysqlAPI mysql.MysqlAPI

func  GetTagsFromDBMetadata(meta *types.DataBaseMetadata) (tags []spec.Tag) {
	tags = make([]spec.Tag, 0)
	for _, t := range meta.Tables {
		if t.TableType=="VIEW" && t.Comment!="" {

			 t.Comment=t.Comment
		}
		//else if t.TableType=="BASE TABLE" {
		//	t.Comment="表"
		//}
		tags = append(tags, spec.Tag{TagProps: spec.TagProps{Name: t.TableName, Description: t.Comment}})
	}
	tags = append(tags, spec.Tag{TagProps: spec.TagProps{Name: "metadata", Description: "元数据"}})
	return
}
