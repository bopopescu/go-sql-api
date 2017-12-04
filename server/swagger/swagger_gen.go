package swagger

import (
	"github.com/go-openapi/spec"
	types    "github.com/shiyongabc/go-mysql-api/types"
)

func GenSwaggerFromDBMetadata(dbMetadata *types.DataBaseMetadata) (s *spec.Swagger) {
	s = &spec.Swagger{}
	s.SwaggerProps = spec.SwaggerProps{}
	s.Swagger = "2.0"
	s.Schemes = []string{"http"}
	s.Tags = GetTagsFromDBMetadata(dbMetadata)
	s.Info = NewSwaggerInfo(dbMetadata, "1.0.0")
	s.Definitions = SwaggerDefinationsFromDabaseMetadata(dbMetadata)
	s.Paths = &spec.Paths{Paths: SwaggerPathsFromDatabaseMetadata(dbMetadata)}
	return
}
