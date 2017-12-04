package mysql

import (
	json "encoding/json"
	easyjson "github.com/mailru/easyjson"
	jlexer "github.com/mailru/easyjson/jlexer"
	jwriter "github.com/mailru/easyjson/jwriter"
)

// suppress unused package warning
var (
	_ *json.RawMessage
	_ *jlexer.Lexer
	_ *jwriter.Writer
	_ easyjson.Marshaler
)

func easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes(in *jlexer.Lexer, out *WhereOperation) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "Operation":
			out.Operation = string(in.String())
		case "Value":
			if m, ok := out.Value.(easyjson.Unmarshaler); ok {
				m.UnmarshalEasyJSON(in)
			} else if m, ok := out.Value.(json.Unmarshaler); ok {
				_ = m.UnmarshalJSON(in.Raw())
			} else {
				out.Value = in.Interface()
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes(out *jwriter.Writer, in WhereOperation) {
	out.RawByte('{')
	first := true
	_ = first
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Operation\":")
	out.String(string(in.Operation))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Value\":")
	if m, ok := in.Value.(easyjson.Marshaler); ok {
		m.MarshalEasyJSON(out)
	} else if m, ok := in.Value.(json.Marshaler); ok {
		out.Raw(m.MarshalJSON())
	} else {
		out.Raw(json.Marshal(in.Value))
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v WhereOperation) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v WhereOperation) MarshalEasyJSON(w *jwriter.Writer) {
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *WhereOperation) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *WhereOperation) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes(l, v)
}
func easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes1(in *jlexer.Lexer, out *TableMetadata) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "table_name":
			out.TableName = string(in.String())
		case "table_type":
			out.TableType = string(in.String())
		case "table_rows":
			out.TableRows = int64(in.Int64())
		case "current_increment":
			out.CurrentIncre = int64(in.Int64())
		case "comment":
			out.Comment = string(in.String())
		case "columns":
			if in.IsNull() {
				in.Skip()
				out.Columns = nil
			} else {
				in.Delim('[')
				if out.Columns == nil {
					if !in.IsDelim(']') {
						out.Columns = make([]*ColumnMetadata, 0, 8)
					} else {
						out.Columns = []*ColumnMetadata{}
					}
				} else {
					out.Columns = (out.Columns)[:0]
				}
				for !in.IsDelim(']') {
					var v1 *ColumnMetadata
					if in.IsNull() {
						in.Skip()
						v1 = nil
					} else {
						if v1 == nil {
							v1 = new(ColumnMetadata)
						}
						if data := in.Raw(); in.Ok() {
							in.AddError((*v1).UnmarshalJSON(data))
						}
					}
					out.Columns = append(out.Columns, v1)
					in.WantComma()
				}
				in.Delim(']')
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes1(out *jwriter.Writer, in TableMetadata) {
	out.RawByte('{')
	first := true
	_ = first
	if in.TableName != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"table_name\":")
		out.String(string(in.TableName))
	}
	if in.TableType != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"table_type\":")
		out.String(string(in.TableType))
	}
	if in.TableRows != 0 {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"table_rows\":")
		out.Int64(int64(in.TableRows))
	}
	if in.CurrentIncre != 0 {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"current_increment\":")
		out.Int64(int64(in.CurrentIncre))
	}
	if in.Comment != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"comment\":")
		out.String(string(in.Comment))
	}
	if len(in.Columns) != 0 {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"columns\":")
		if in.Columns == nil && (out.Flags&jwriter.NilSliceAsEmpty) == 0 {
			out.RawString("null")
		} else {
			out.RawByte('[')
			for v2, v3 := range in.Columns {
				if v2 > 0 {
					out.RawByte(',')
				}
				if v3 == nil {
					out.RawString("null")
				} else {
					out.Raw((*v3).MarshalJSON())
				}
			}
			out.RawByte(']')
		}
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v TableMetadata) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes1(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v TableMetadata) MarshalEasyJSON(w *jwriter.Writer) {
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes1(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *TableMetadata) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes1(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *TableMetadata) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes1(l, v)
}
func easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes2(in *jlexer.Lexer, out *QueryOption) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "Table":
			out.Table = string(in.String())
		case "Id":
			out.Id = string(in.String())
		case "Index":
			out.Index = int(in.Int())
		case "Limit":
			out.Limit = int(in.Int())
		case "Offset":
			out.Offset = int(in.Int())
		case "Fields":
			if in.IsNull() {
				in.Skip()
				out.Fields = nil
			} else {
				in.Delim('[')
				if out.Fields == nil {
					if !in.IsDelim(']') {
						out.Fields = make([]string, 0, 4)
					} else {
						out.Fields = []string{}
					}
				} else {
					out.Fields = (out.Fields)[:0]
				}
				for !in.IsDelim(']') {
					var v4 string
					v4 = string(in.String())
					out.Fields = append(out.Fields, v4)
					in.WantComma()
				}
				in.Delim(']')
			}
		case "Links":
			if in.IsNull() {
				in.Skip()
				out.Links = nil
			} else {
				in.Delim('[')
				if out.Links == nil {
					if !in.IsDelim(']') {
						out.Links = make([]string, 0, 4)
					} else {
						out.Links = []string{}
					}
				} else {
					out.Links = (out.Links)[:0]
				}
				for !in.IsDelim(']') {
					var v5 string
					v5 = string(in.String())
					out.Links = append(out.Links, v5)
					in.WantComma()
				}
				in.Delim(']')
			}
		case "Wheres":
			if in.IsNull() {
				in.Skip()
			} else {
				in.Delim('{')
				if !in.IsDelim('}') {
					out.Wheres = make(map[string]WhereOperation)
				} else {
					out.Wheres = nil
				}
				for !in.IsDelim('}') {
					key := string(in.String())
					in.WantColon()
					var v6 WhereOperation
					if data := in.Raw(); in.Ok() {
						in.AddError((v6).UnmarshalJSON(data))
					}
					(out.Wheres)[key] = v6
					in.WantComma()
				}
				in.Delim('}')
			}
		case "Search":
			out.Search = string(in.String())
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes2(out *jwriter.Writer, in QueryOption) {
	out.RawByte('{')
	first := true
	_ = first
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Table\":")
	out.String(string(in.Table))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Id\":")
	out.String(string(in.Id))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Index\":")
	out.Int(int(in.Index))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Limit\":")
	out.Int(int(in.Limit))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Offset\":")
	out.Int(int(in.Offset))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Fields\":")
	if in.Fields == nil && (out.Flags&jwriter.NilSliceAsEmpty) == 0 {
		out.RawString("null")
	} else {
		out.RawByte('[')
		for v7, v8 := range in.Fields {
			if v7 > 0 {
				out.RawByte(',')
			}
			out.String(string(v8))
		}
		out.RawByte(']')
	}
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Links\":")
	if in.Links == nil && (out.Flags&jwriter.NilSliceAsEmpty) == 0 {
		out.RawString("null")
	} else {
		out.RawByte('[')
		for v9, v10 := range in.Links {
			if v9 > 0 {
				out.RawByte(',')
			}
			out.String(string(v10))
		}
		out.RawByte(']')
	}
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Wheres\":")
	if in.Wheres == nil && (out.Flags&jwriter.NilMapAsEmpty) == 0 {
		out.RawString(`null`)
	} else {
		out.RawByte('{')
		v11First := true
		for v11Name, v11Value := range in.Wheres {
			if !v11First {
				out.RawByte(',')
			}
			v11First = false
			out.String(string(v11Name))
			out.RawByte(':')
			out.Raw((v11Value).MarshalJSON())
		}
		out.RawByte('}')
	}
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"Search\":")
	out.String(string(in.Search))
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v QueryOption) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes2(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v QueryOption) MarshalEasyJSON(w *jwriter.Writer) {
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes2(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *QueryOption) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes2(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *QueryOption) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes2(l, v)
}
func easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes3(in *jlexer.Lexer, out *Paginator) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "pageIndex":
			out.PageIndex = int(in.Int())
		case "pageSize":
			out.PageSize = int(in.Int())
		case "totalPages":
			out.TotalPages = int(in.Int())
		case "totalCount":
			out.TotalCount = int(in.Int())
		case "data":
			if m, ok := out.Data.(easyjson.Unmarshaler); ok {
				m.UnmarshalEasyJSON(in)
			} else if m, ok := out.Data.(json.Unmarshaler); ok {
				_ = m.UnmarshalJSON(in.Raw())
			} else {
				out.Data = in.Interface()
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes3(out *jwriter.Writer, in Paginator) {
	out.RawByte('{')
	first := true
	_ = first
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"pageIndex\":")
	out.Int(int(in.PageIndex))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"pageSize\":")
	out.Int(int(in.PageSize))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"totalPages\":")
	out.Int(int(in.TotalPages))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"totalCount\":")
	out.Int(int(in.TotalCount))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"data\":")
	if m, ok := in.Data.(easyjson.Marshaler); ok {
		m.MarshalEasyJSON(out)
	} else if m, ok := in.Data.(json.Marshaler); ok {
		out.Raw(m.MarshalJSON())
	} else {
		out.Raw(json.Marshal(in.Data))
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v Paginator) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes3(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v Paginator) MarshalEasyJSON(w *jwriter.Writer) {
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes3(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *Paginator) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes3(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *Paginator) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes3(l, v)
}
func easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes4(in *jlexer.Lexer, out *ErrorMessage) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "error":
			out.ErrorTitle = string(in.String())
		case "error_description":
			out.ErrorDescription = string(in.String())
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes4(out *jwriter.Writer, in ErrorMessage) {
	out.RawByte('{')
	first := true
	_ = first
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"error\":")
	out.String(string(in.ErrorTitle))
	if !first {
		out.RawByte(',')
	}
	first = false
	out.RawString("\"error_description\":")
	out.String(string(in.ErrorDescription))
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v ErrorMessage) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes4(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v ErrorMessage) MarshalEasyJSON(w *jwriter.Writer) {
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes4(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *ErrorMessage) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes4(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *ErrorMessage) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes4(l, v)
}
func easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes5(in *jlexer.Lexer, out *DataBaseMetadata) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "database_name":
			out.DatabaseName = string(in.String())
		case "tables":
			if in.IsNull() {
				in.Skip()
				out.Tables = nil
			} else {
				in.Delim('[')
				if out.Tables == nil {
					if !in.IsDelim(']') {
						out.Tables = make([]*TableMetadata, 0, 8)
					} else {
						out.Tables = []*TableMetadata{}
					}
				} else {
					out.Tables = (out.Tables)[:0]
				}
				for !in.IsDelim(']') {
					var v12 *TableMetadata
					if in.IsNull() {
						in.Skip()
						v12 = nil
					} else {
						if v12 == nil {
							v12 = new(TableMetadata)
						}
						if data := in.Raw(); in.Ok() {
							in.AddError((*v12).UnmarshalJSON(data))
						}
					}
					out.Tables = append(out.Tables, v12)
					in.WantComma()
				}
				in.Delim(']')
			}
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes5(out *jwriter.Writer, in DataBaseMetadata) {
	out.RawByte('{')
	first := true
	_ = first
	if in.DatabaseName != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"database_name\":")
		out.String(string(in.DatabaseName))
	}
	if len(in.Tables) != 0 {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"tables\":")
		if in.Tables == nil && (out.Flags&jwriter.NilSliceAsEmpty) == 0 {
			out.RawString("null")
		} else {
			out.RawByte('[')
			for v13, v14 := range in.Tables {
				if v13 > 0 {
					out.RawByte(',')
				}
				if v14 == nil {
					out.RawString("null")
				} else {
					out.Raw((*v14).MarshalJSON())
				}
			}
			out.RawByte(']')
		}
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v DataBaseMetadata) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes5(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v DataBaseMetadata) MarshalEasyJSON(w *jwriter.Writer) {
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes5(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *DataBaseMetadata) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes5(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *DataBaseMetadata) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes5(l, v)
}
func easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes6(in *jlexer.Lexer, out *ColumnMetadata) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "column_name":
			out.ColumnName = string(in.String())
		case "column_type":
			out.ColumnType = string(in.String())
		case "nullable":
			out.NullAble = string(in.String())
		case "key":
			out.Key = string(in.String())
		case "default_value":
			out.DefaultValue = string(in.String())
		case "extra":
			out.Extra = string(in.String())
		case "oridinal_sequence":
			out.OridinalSequence = int64(in.Int64())
		case "data_type":
			out.DataType = string(in.String())
		case "comment":
			out.Comment = string(in.String())
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
func easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes6(out *jwriter.Writer, in ColumnMetadata) {
	out.RawByte('{')
	first := true
	_ = first
	if in.ColumnName != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"column_name\":")
		out.String(string(in.ColumnName))
	}
	if in.ColumnType != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"column_type\":")
		out.String(string(in.ColumnType))
	}
	if in.NullAble != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"nullable\":")
		out.String(string(in.NullAble))
	}
	if in.Key != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"key\":")
		out.String(string(in.Key))
	}
	if in.DefaultValue != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"default_value\":")
		out.String(string(in.DefaultValue))
	}
	if in.Extra != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"extra\":")
		out.String(string(in.Extra))
	}
	if in.OridinalSequence != 0 {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"oridinal_sequence\":")
		out.Int64(int64(in.OridinalSequence))
	}
	if in.DataType != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"data_type\":")
		out.String(string(in.DataType))
	}
	if in.Comment != "" {
		if !first {
			out.RawByte(',')
		}
		first = false
		out.RawString("\"comment\":")
		out.String(string(in.Comment))
	}
	out.RawByte('}')
}

// MarshalJSON supports json.Marshaler interface
func (v ColumnMetadata) MarshalJSON() ([]byte, error) {
	w := jwriter.Writer{}
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes6(&w, v)
	return w.Buffer.BuildBytes(), w.Error
}

// MarshalEasyJSON supports easyjson.Marshaler interface
func (v ColumnMetadata) MarshalEasyJSON(w *jwriter.Writer) {
	easyjsonBc289ab0EncodeGithubComXuybinGoMysqlApiTypes6(w, v)
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *ColumnMetadata) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes6(&r, v)
	return r.Error()
}

// UnmarshalEasyJSON supports easyjson.Unmarshaler interface
func (v *ColumnMetadata) UnmarshalEasyJSON(l *jlexer.Lexer) {
	easyjsonBc289ab0DecodeGithubComXuybinGoMysqlApiTypes6(l, v)
}