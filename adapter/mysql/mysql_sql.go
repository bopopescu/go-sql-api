package mysql

import (
	"fmt"

	. "github.com/shiyongabc/go-sql-api/types"
	"gopkg.in/doug-martin/goqu.v4"
	_ "gopkg.in/doug-martin/goqu.v4/adapters/mysql"
	"regexp"
	"sort"
	"strconv"
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
		fmt.Printf("err=",err)
		return
	}
	sql, _, err = builder.ToSql()
	if opt.GroupFields!=nil{
		var count int
		var groupFields string
		for _, f := range opt.GroupFields {
			if count==(len(opt.GroupFields)-1){
				groupFields = groupFields+f
			}else{
				groupFields = f+","
			}
			count=count+1
		}
		if strings.Contains(sql,"order by") &&groupFields!=""{
			sql=strings.Replace(sql,"order by","group by "+groupFields+" order by",-1)
		}else if strings.Contains(sql,"ORDER BY")&&groupFields!=""{
			sql=strings.Replace(sql,"ORDER BY","group by "+groupFields+" ORDER BY",-1)
		}else if strings.Contains(sql,"LIMIT")&&groupFields!=""{
			sql=strings.Replace(sql,"LIMIT","group by "+groupFields+" LIMIT",-1)
		}else{
			if groupFields!=""{
				sql=sql+" "+"group by "+groupFields
			}
		}


	}

	//替换掉` 兼容聚合函数求出的值 作为新的列
	sql=strings.Replace(sql,"`","",-1)
	sql=strings.Replace(sql,"\\","",-1)
	sql=strings.Replace(sql,"IS 'NULL'","IS NULL",-1)
	sql=strings.Replace(sql,"IS 'null'","IS NULL",-1)
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
	if len(opt.GroupFields)>0{
		sql=strings.Replace(sql,"`_placeholder_`","COUNT(distinct("+opt.GroupFields[0]+")) as TotalCount",-1)
	}else{
		sql=strings.Replace(sql,"`_placeholder_`","COUNT(*) as TotalCount",-1)
	}

	sql=strings.Replace(sql,"\\","",-1)
	sql=strings.Replace(sql,"IS 'NULL'","IS NULL",-1)
	sql=strings.Replace(sql,"IS 'null'","IS NULL",-1)
	//sql="SELECT `user_id`, SUM(account_log.account_funds) as totalFunds FROM `account_log`"
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
	if opt.GroupFields!=nil{
		var count int
		var groupFields string
		for _, f := range opt.GroupFields {
			if count==(len(opt.GroupFields)-1){
				groupFields = groupFields+f
			}else{
				groupFields = f+","
			}
			count=count+1
		}
		if strings.Contains(sql,"order by")&&groupFields!=""{
			sql=strings.Replace(sql,"order by","group by "+groupFields+" order by",-1)
		}else if strings.Contains(sql,"ORDER BY")&&groupFields!=""{
			sql=strings.Replace(sql,"ORDER BY","group by "+groupFields+" ORDER BY",-1)
		}else if strings.Contains(sql,"LIMIT")&&groupFields!=""{
			sql=strings.Replace(sql,"LIMIT","group by "+groupFields+" LIMIT",-1)
		}else {
			if groupFields!=""{
				sql=sql+" "+"group by "+groupFields
			}

		}

	}

	sql=strings.Replace(sql,"`","",-1)
	sql=strings.Replace(sql,"\\","",-1)
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
		//operation:="eq"
		if strings.Contains(f,".lt"){
			f=strings.Replace(f,".lt","",-1)
			//operation="lt"
		}
		if strings.Contains(f,".gt"){
			f=strings.Replace(f,".gt","",-1)
			//operation="gt"
		}

		//builder = builder.Where(goqu.Ex{f: v.Value})
		builder = builder.Where(goqu.ExOr{f:goqu.Op{v.Operation: v.Value}})
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
	//rs.Pluck("","SUM(account_funds)")
	//rs.As("SUM(account_funds)")
	if opt.Limit != 0 {
		rs = rs.Limit(uint(opt.Limit))
	}
	if opt.Offset != 0 {
		rs = rs.Offset(uint(opt.Offset))
	}
	groupFuncs:=strings.Split(opt.GroupFunc,",")
	fs := make([]interface{}, len(opt.Fields))
	 if opt.GroupFunc!=""{
		 fs = make([]interface{}, len(opt.Fields)+len(groupFuncs))
	}
	var index int
	if opt.Fields != nil {
		for idx, f := range opt.Fields {
			fs[idx] = f
			index=idx
		}

	}
	if opt.GroupFunc!=""{

		for i,item:=range groupFuncs{
			if strings.Contains(item,"|"){
				item=strings.Replace(item,"|",",",-1)
			}
			if len(opt.Fields)>0{
				if i>=1{
					iStr:=strconv.Itoa(i)
					fs[i+index+1] = item+" as p"+iStr
				}else{
					fs[i+index+1] = item+" as p"
				}

			}else{
				fs[i+index] = item+" as p"
			}
		}


		rs = rs.Select(fs...)
	}else{
		rs = rs.Select(fs...)
	}


	for f, w := range opt.Wheres {
		// check field exist
		if strings.Contains(f,".gte"){
			f=strings.Replace(f,".gte","",-1)
		}
		if strings.Contains(f,".`gte`"){
			f=strings.Replace(f,".`gte`","",-1)
		}

		if strings.Contains(f,".gt"){
			f=strings.Replace(f,".gt","",-1)
		}
		if strings.Contains(f,".`gt`"){
			f=strings.Replace(f,".`gt`","",-1)
		}
		if strings.Contains(f,".lte"){
			f=strings.Replace(f,".lte","",-1)
		}
		if strings.Contains(f,".`lte`"){
			f=strings.Replace(f,".`lte`","",-1)
		}

		if strings.Contains(f,".lt"){
			f=strings.Replace(f,".lt","",-1)
		}
		if strings.Contains(f,".`lt`"){
			f=strings.Replace(f,".`lt`","",-1)
		}




		//rs = rs.Where(goqu.Or{f:goqu.Op{w.Operation: w.Value}})
		//  (("a" = 10) OR ("b" = 11))
		//rs=rs.Where(goqu.Or(goqu.I("a").Eq(10), goqu.I("b").Eq(11)))
	//	rs = rs.Where(goqu.Or({f:goqu.Op{w.Operation: w.Value}},f:goqu.Op{w.Operation: w.Value}}))

	//switch 	 w.Value.(type){
	//case string:
	//	w.Value=strings.Replace(w.Value.(string),"\\","",-1)
	//}
		rs = rs.Where(goqu.ExOr{f:goqu.Op{w.Operation: w.Value}})

	}
	expressTemp:=goqu.I("a").Eq("1")

//	var list [4]goqu.Expression
	var count int
	ors := make([]goqu.Expression, len(opt.OrWheres))
	for f, w := range opt.OrWheres {

		// check field exist
		if strings.Contains(f,".gte"){
			f=strings.Replace(f,".gte","",-1)
		}
		if strings.Contains(f,".`gte`"){
			f=strings.Replace(f,".`gte`","",-1)
		}

		if strings.Contains(f,".gt"){
			f=strings.Replace(f,".gt","",-1)
		}
		if strings.Contains(f,".`gt`"){
			f=strings.Replace(f,".`gt`","",-1)
		}
		if strings.Contains(f,".lte"){
			f=strings.Replace(f,".lte","",-1)
		}
		if strings.Contains(f,".`lte`"){
			f=strings.Replace(f,".`lte`","",-1)
		}

		if strings.Contains(f,".lt"){
			f=strings.Replace(f,".lt","",-1)
		}
		if strings.Contains(f,".`lt`"){
			f=strings.Replace(f,".`lt`","",-1)
		}

		if w.Value!=nil{
			switch w.Value.(type) {
			case string:
				f=strings.Replace(f,"$"+w.Value.(string),"",-1)
			case float64:
				f=strings.Replace(f,"$"+ fmt.Sprintf("%0.2f", w.Value.(float64)),"",-1)
			}

		}

		//rs = rs.Where(goqu.Or{f:goqu.Op{w.Operation: w.Value}})
		//  (("a" = 10) OR ("b" = 11))

		if w.Operation=="eq"{
			expressTemp=goqu.I(f).Eq(w.Value)
			ors[count]=expressTemp
			count=count+1
			continue
		}else if w.Operation=="neq"{
			expressTemp=goqu.I(f).Neq(w.Value)
			ors[count]=expressTemp
			count=count+1
			continue
		}else if w.Operation=="is"{
			expressTemp=goqu.I(f).Is(w.Value)
			ors[count]=expressTemp
			count=count+1
			continue

		}else if w.Operation=="isNot"{
			expressTemp=goqu.I(f).IsNot(w.Value)
			ors[count]=expressTemp
			count=count+1
			continue

		}else if w.Operation=="like"{
			expressTemp=goqu.I(f).Like(w.Value)
			ors[count]=expressTemp
			count=count+1
			continue
		}else if w.Operation=="in"{
			expressTemp=goqu.I(f).In(w.Value)
			ors[count]=expressTemp
			count=count+1
			continue
		}else if w.Operation=="lt"{
			expressTemp=goqu.I(f).Lt(w.Value)
			ors[count]=expressTemp
			count=count+1
			continue

		}else if w.Operation=="lte"{
			expressTemp=goqu.I(f).Lte(w.Value)
			ors[count]=expressTemp
			count=count+1
			continue


		}else if w.Operation=="gte"{
			expressTemp=goqu.I(f).Gte(w.Value)
			ors[count]=expressTemp
			count=count+1
			continue

		}else if w.Operation=="gt"{
			expressTemp=goqu.I(f).Gt(w.Value)
			ors[count]=expressTemp
			count=count+1
			continue

		}





		//	rs = rs.Where(goqu.Or({f:goqu.Op{w.Operation: w.Value}},f:goqu.Op{w.Operation: w.Value}}))
	//	rs = rs.Where(goqu.ExOr{f:goqu.Op{w.Operation: w.Value}})

	}
	//if list[0]!=nil && list[1]!=nil && list[2]!=nil&& list[3]!=nil{
	//	rs=rs.Where(goqu.Or(list[0], list[1],list[2],list[3]))
	//}else if list[0]!=nil && list[1]!=nil && list[2]!=nil{
	//	rs=rs.Where(goqu.Or(list[0], list[1],list[2]))
	//}else if list[0]!=nil && list[1]!=nil{
	//	rs=rs.Where(goqu.Or(list[0], list[1]))
	//}

	if len(ors)>0{
		rs=rs.Where(goqu.Or(ors...))
	}


	var newMp = make([]string, 0)
	for k, _ := range opt.Orders {
		newMp = append(newMp, k)
	}
	sort.Strings(newMp)
	for _, key := range newMp {
		//fmt.Println("根据key排序后的新集合》》   key:", key, "    value:", opt.Orders[key])
		columnName:=key
		var columnTemp string
		columnTemp=columnName
		var orderTable string

		if strings.Contains(columnName,"."){
			arr:=strings.Split(columnName,".")
			orderTable=arr[0]
			columnTemp=arr[1]
		}
		r := regexp.MustCompile("^(N)[0-9]([\\w]+)")
	//	r.FindString(columnName)
		if r.FindString(columnTemp)!=""{
			columnName=columnTemp[2:]
			if "line_number"==columnName && !strings.Contains(opt.Table,"merge"){
				orderTable=orderTable+"_detail"
			}
			orderColumn:=orderTable+"."+columnName

			if "DESC"==strings.ToUpper(opt.Orders[key]){
				rs=rs.OrderAppend(goqu.I(orderColumn).Desc())
			}else{
				rs=rs.OrderAppend(goqu.I(orderColumn).Asc())
			}
		}else{
			if "DESC"==strings.ToUpper(opt.Orders[key]){
				rs=rs.OrderAppend(goqu.I(key).Desc())
			}else{
				rs=rs.OrderAppend(goqu.I(key).Asc())
			}
		}

	}



	for _, l := range opt.Links {
		refT := l
		//opt.ExtendedMap
		for _,item:=range opt.ExtendedArr{
			refK:=item["ref_k"].(string)
			priK:=item["pri_k"].(string)
			rs = rs.InnerJoin(goqu.I(refT), goqu.On(goqu.I(fmt.Sprintf("%s.%s", refT, refK)).Eq(goqu.I(fmt.Sprintf("%s.%s", priT, priK)))))
			return
		}
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
