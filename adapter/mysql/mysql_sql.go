package mysql

import (
	"fmt"

	"gopkg.in/doug-martin/goqu.v4"
	. "github.com/shiyongabc/go-mysql-api/types"
	_ "gopkg.in/doug-martin/goqu.v4/adapters/mysql"
	"strings"
)

// SQL return sqls by sql builder
type SQL struct {
	sqlBuilder *goqu.Database
	dbMeta     *DataBaseMetadata
}

func (s *SQL) getPriKeyNameOf(tableName string) (priKeyName string,err error) {
	if(!s.dbMeta.HaveTable(tableName)){
		err = fmt.Errorf("Error 1146: Table '%s.%s' doesn't exist", s.dbMeta.DatabaseName,tableName)
	}else {
		primaryColumns:=s.dbMeta.GetTableMeta(tableName).GetPrimaryColumns()
		if(len(primaryColumns)==0){
			err = fmt.Errorf("Table '%s.%s' doesn't have a primary key", s.dbMeta.DatabaseName,tableName)
		}else if(len(primaryColumns)>1){
			err = fmt.Errorf("Table '%s.%s' doesn't has more than one primary key", s.dbMeta.DatabaseName,tableName)
		}else {
			priKeyName=primaryColumns[0].ColumnName
		}
	}
	return
}
func (s *SQL) getAllPriKeyNameOf(tableName string) (primaryColumnNames []string,err error) {
	if(!s.dbMeta.HaveTable(tableName)){
		err = fmt.Errorf("Error 1146: Table '%s.%s' doesn't exist", s.dbMeta.DatabaseName,tableName)
	}else {
		for _, primaryColumn := range s.dbMeta.GetTableMeta(tableName).GetPrimaryColumns(){
			primaryColumnNames=append(primaryColumnNames ,primaryColumn.ColumnName)
		}
	}
	return
}
// GetByTable with filter
func (s *SQL) GetByTable(opt QueryOption) (sql string, err error) {
	builder := s.sqlBuilder.From(opt.Table)
	builder,err = s.configBuilder(builder, opt.Table, opt)
	if(err!=nil){
		return
	}
	sql, _, err = builder.ToSql()
	return
}
func (s *SQL) GetByTableTotalCount(opt QueryOption) (sql string, err error) {
	builder := s.sqlBuilder.From(opt.Table)
	builder,err = s.configBuilder(builder, opt.Table, opt)
	if(err!=nil){
		return
	}
	builder =builder.ClearSelect()
	builder = builder.Select("_placeholder_")
	builder = builder.ClearLimit()
	builder = builder.ClearOffset()
	sql, _, err = builder.ToSql()
	sql=strings.Replace(sql,"`_placeholder_`","COUNT(*) as TotalCount",-1)
	return
}

// GetByTableAndID for specific record in Table
func (s *SQL) GetByTableAndID(opt QueryOption) (sql string, err error) {
	priKeyNames,err := s.getAllPriKeyNameOf(opt.Table)
	if(err!=nil){
		return
	}
	opt.Id=strings.Replace(opt.Id, "%2c", ",", -1)
	opt.Id=strings.Replace(opt.Id, "%2C", ",", -1)
	ids:=strings.Split(opt.Id,",")
	if len(priKeyNames) ==0 {
		err = fmt.Errorf("Table `%s` dont have primary key !", opt.Table)
		return
	} else if(len(ids)!=len(priKeyNames)){
		err=fmt.Errorf("'%v' and '%v' length is different ", strings.Join(priKeyNames,","),strings.Join(ids,","))
		return sql, err
	}
	builder:= s.sqlBuilder.From(opt.Table)
	for i, priKeyName := range priKeyNames{
		builder = builder.Where(goqu.Ex{priKeyName: ids[i] })
	}
	builder ,err= s.configBuilder(builder, opt.Table, opt)
	if(err!=nil){
		return
	}
	sql, _, err = builder.ToSql()
	return sql, err
}

// UpdateByTable for update specific record by id
func (s *SQL) UpdateByTableAndId(tableName string, id interface{}, record map[string]interface{}) (sql string, err error) {
	priKeyNames,err := s.getAllPriKeyNameOf(tableName)
	if(err!=nil){
		return
	}
	idSrt:=strings.Replace(id.(string), "%2c", ",", -1)
	idSrt=strings.Replace(idSrt, "%2C", ",", -1)
	ids:=strings.Split(idSrt,",")
	if len(priKeyNames) ==0 {
		err = fmt.Errorf("Table `%s` dont have primary key !", tableName)
		return
	} else if(len(ids)!=len(priKeyNames)){
		err=fmt.Errorf("'%v' and '%v' length is different ", strings.Join(priKeyNames,","),strings.Join(ids,","))
		return sql, err
	}
	builder := s.sqlBuilder.From(tableName)
	for i, priKeyName := range priKeyNames{
		builder = builder.Where(goqu.Ex{priKeyName: ids[i]})
	}
	sql, _, err = builder.ToUpdateSql(record)
	return
}

func (s *SQL) UpdateByTableAndFields(tableName string, where map[string]WhereOperation, record map[string]interface{}) (sql string, err error) {

	if(where==nil){
		err = fmt.Errorf("update table `%s` must have where !", tableName)
		return
	}

	builder := s.sqlBuilder.From(tableName)
	for f, v := range where{
		operation:="eq"
		if strings.Contains(f,".lt"){
			f=strings.Replace(f,".lt","",-1)
			operation="lt"
		}
		if strings.Contains(f,".gt"){
			f=strings.Replace(f,".gt","",-1)
			operation="gt"
		}
		if strings.Contains(f,".like"){
			f=strings.Replace(f,".like","",-1)
			operation="like"
		}
		if strings.Contains(f,".in"){
			f=strings.Replace(f,".in","",-1)
			operation="in"
		}
		if strings.Contains(f,".notin"){
			f=strings.Replace(f,".notin","",-1)
			operation="notin"
		}
		//builder = builder.Where(goqu.Ex{f: v.Value})
		builder = builder.Where(goqu.ExOr{f:goqu.Op{operation: v.Value}})
		//rs = rs.Where(goqu.ExOr{f:goqu.Op{w.Operation: w.Value}})

	}
	sql, _, err = builder.ToUpdateSql(record)
	return
}

// InsertByTable and record map
func (s *SQL) InsertByTable(tableName string, record map[string]interface{}) (sql string, err error) {
	sql, _, err = s.sqlBuilder.From(tableName).Where().ToInsertSql(record)
	return
}

// DeleteByTable by where
func (s *SQL) DeleteByTable(tableName string, mWhere map[string]interface{}) (sql string, err error) {
	if len(mWhere) ==0 {
		err = fmt.Errorf("Delete Table `%s` dont have any where value !", tableName)
		return
	}
	builder := s.sqlBuilder.From(tableName)
	for k, v := range mWhere {
		builder = builder.Where(goqu.Ex{k: v})
	}
	sql = builder.Delete().Sql
	return
}

// DeleteByTableAndId
func (s *SQL) DeleteByTableAndId(tableName string, id interface{}) (sql string, err error) {
	priKeyNames,err := s.getAllPriKeyNameOf(tableName)
	if(err!=nil){
		return
	}
	idSrt:=strings.Replace(id.(string), "%2c", ",", -1)
	idSrt=strings.Replace(idSrt, "%2C", ",", -1)
	ids:=strings.Split(idSrt,",")

	if len(priKeyNames) ==0 {
		err = fmt.Errorf("Table `%s` dont have primary key !", tableName)
		return
	} else if(len(ids)!=len(priKeyNames)){
		err=fmt.Errorf("'%v' and '%v' length is different ", strings.Join(priKeyNames,","),strings.Join(ids,","))
		return sql, err
	}
	builder := s.sqlBuilder.From(tableName)
	for i, priKeyName := range priKeyNames{
		builder = builder.Where(goqu.Ex{priKeyName: ids[i]})
	}
	sql, _, err = builder.ToDeleteSql()
	return

}

func (s *SQL) configBuilder(builder *goqu.Dataset, priT string, opt QueryOption) (rs *goqu.Dataset,err error) {
	rs = builder
	if opt.Limit != 0 {
		rs = rs.Limit(uint(opt.Limit))
	}
	if opt.Offset != 0 {
		rs = rs.Offset(uint(opt.Offset))
	}
	if opt.Fields != nil {
		fs := make([]interface{}, len(opt.Fields))
		for idx, f := range opt.Fields {
			fs[idx] = f
		}
		rs = rs.Select(fs...)
	}
	for f, w := range opt.Wheres {
		// check field exist
		if strings.Contains(f,".gt"){
			f=strings.Replace(f,".gt","",-1)
		}
		if strings.Contains(f,".`gt`"){
			f=strings.Replace(f,".`gt`","",-1)
		}
		if strings.Contains(f,".lt"){
			f=strings.Replace(f,".lt","",-1)
		}
		if strings.Contains(f,".`lt`"){
			f=strings.Replace(f,".`lt`","",-1)
		}
		rs = rs.Where(goqu.ExOr{f:goqu.Op{w.Operation: w.Value}})

	}
	for orderColumn, orderStr := range opt.Orders {
		// check field exist
		if "DESC"==strings.ToUpper(orderStr){
			rs=rs.OrderAppend(goqu.I(orderColumn).Desc())
		}else{
			rs=rs.OrderAppend(goqu.I(orderColumn).Asc())
		}


	}


	for _, l := range opt.Links {
		refT := l
		//multi-PriKey or No-PriKey
		refK ,err1:= s.getPriKeyNameOf(refT)
		if(err1!=nil){
			err=err1
			return
		}
		priK ,err1:= s.getPriKeyNameOf(priT)
		if(err1!=nil){
			err=err1
			return
		}
		if s.dbMeta.TableHaveField(priT, refK) {
			rs = rs.InnerJoin(goqu.I(refT), goqu.On(goqu.I(fmt.Sprintf("%s.%s", refT, refK)).Eq(goqu.I(fmt.Sprintf("%s.%s", priT, refK)))))
		}
		if s.dbMeta.TableHaveField(refT, priK) {
			rs = rs.InnerJoin(goqu.I(refT), goqu.On(goqu.I(fmt.Sprintf("%s.%s", refT, priK)).Eq(goqu.I(fmt.Sprintf("%s.%s", priT, priK)))))
		}
	}
	if opt.Search != "" {
		searchEx := goqu.ExOr{}
		for _, c := range s.dbMeta.GetTableMeta(opt.Table).Columns {
			searchEx[c.ColumnName] = goqu.Op{"like": fmt.Sprintf("%%%s%%", opt.Search)}
		}
		rs = rs.Where(searchEx)
	}
	return
}
