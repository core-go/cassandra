package cassandra

import (
	"context"
	"fmt"
	"github.com/apache/cassandra-gocql-driver"
	"reflect"
	"strconv"
	"strings"
)

func Init(modelType reflect.Type) (map[string]int, *Schema, map[string]string, []string, []string, string, error) {
	fieldsIndex, err := GetColumnIndexes(modelType)
	if err != nil {
		return nil, nil, nil, nil, nil, "", err
	}
	schema := CreateSchema(modelType)
	fields := BuildFieldsBySchema(schema)
	jsonColumnMap := MakeJsonColumnMap(modelType)
	keys, arr := FindPrimaryKeys(modelType)
	return fieldsIndex, schema, jsonColumnMap, keys, arr, fields, nil
}

type Writer struct {
	*Loader
	jsonColumnMap  map[string]string
	Mapper         Mapper
	versionField   string
	versionIndex   int
	versionDBField string
	schema         *Schema
}

func NewWriter(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, options ...Mapper) (*Writer, error) {
	return NewWriterWithVersion(db, tableName, modelType, "", options...)
}
func NewWriterWithVersion(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, versionField string, options ...Mapper) (*Writer, error) {
	var mapper Mapper
	if len(options) > 0 {
		mapper = options[0]
	}
	var loader *Loader
	var err error
	if mapper != nil {
		loader, err = NewLoader(db, tableName, modelType, mapper.DbToModel)
	} else {
		loader, err = NewLoader(db, tableName, modelType, nil)
	}
	if err != nil {
		return nil, err
	}
	schema := CreateSchema(modelType)
	jsonColumnMap := MakeJsonColumnMap(modelType)
	if len(versionField) > 0 {
		index := FindFieldIndex(modelType, versionField)
		if index >= 0 {
			_, dbFieldName, exist := GetFieldByIndex(modelType, index)
			if !exist {
				dbFieldName = strings.ToLower(versionField)
			}
			return &Writer{Loader: loader, schema: schema, Mapper: mapper, jsonColumnMap: jsonColumnMap, versionField: versionField, versionIndex: index, versionDBField: dbFieldName}, nil
		}
	}
	return &Writer{Loader: loader, schema: schema, Mapper: mapper, jsonColumnMap: jsonColumnMap, versionField: versionField, versionIndex: -1}, nil
}
func (s *Writer) Insert(ctx context.Context, model interface{}) (int64, error) {
	var m interface{}
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, model)
		if err != nil {
			return 0, err
		}
		m = m2
	} else {
		m = model
	}
	query, values := BuildToInsertWithVersion(s.table, m, s.versionIndex, false, s.schema)
	ses, err := s.DB.CreateSession()
	if err != nil {
		return -1, err
	}
	defer ses.Close()
	er2 := Exec(ses, query, values...)
	if er2 == nil {
		return 1, er2
	}
	return 0, er2
}
func (s *Writer) Update(ctx context.Context, model interface{}) (int64, error) {
	var m interface{}
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
		m = m2
	} else {
		m = model
	}
	query, values := BuildToUpdateWithVersion(s.table, m, s.versionIndex, s.schema)
	ses, err := s.DB.CreateSession()
	if err != nil {
		return -1, err
	}
	defer ses.Close()
	er2 := Exec(ses, query, values...)
	if er2 == nil {
		return 1, er2
	}
	return 0, er2
}
func (s *Writer) Save(ctx context.Context, model interface{}) (int64, error) {
	var m interface{}
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
		m = m2
	} else {
		m = model
	}
	query, values := BuildToSave(s.table, m, s.schema)
	ses, err := s.DB.CreateSession()
	if err != nil {
		return -1, err
	}
	defer ses.Close()
	er2 := Exec(ses, query, values...)
	if er2 == nil {
		return 1, er2
	}
	return 0, er2
}
func (s *Writer) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	if s.Mapper != nil {
		_, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
	}
	MapToDB(&model, s.modelType)
	dbColumnMap := JSONToColumns(model, s.jsonColumnMap)
	query, values := BuildToPatchWithVersion(s.table, dbColumnMap, s.schema.SKeys, s.versionDBField)
	ses, err := s.DB.CreateSession()
	if err != nil {
		return -1, err
	}
	defer ses.Close()
	er2 := Exec(ses, query, values...)
	if er2 == nil {
		return 1, er2
	}
	return 0, er2
}
func MapToDB(model *map[string]interface{}, modelType reflect.Type) {
	for colName, value := range *model {
		if boolValue, boolOk := value.(bool); boolOk {
			index := GetIndexByTag("json", colName, modelType)
			if index > -1 {
				valueS := modelType.Field(index).Tag.Get(strconv.FormatBool(boolValue))
				valueInt, err := strconv.Atoi(valueS)
				if err != nil {
					(*model)[colName] = valueS
				} else {
					(*model)[colName] = valueInt
				}
				continue
			}
		}
		(*model)[colName] = value
	}
}
func (s *Writer) Delete(ctx context.Context, id interface{}) (int64, error) {
	query := BuildQueryById(id, s.modelType, s.keys[0])
	sql, values := BuildToDelete(s.table, query)
	ses, err := s.DB.CreateSession()
	if err != nil {
		return -1, err
	}
	defer ses.Close()
	er2 := Exec(ses, sql, values...)
	if er2 == nil {
		return 1, er2
	}
	return 0, er2
}

type Mapper interface {
	DbToModel(ctx context.Context, model interface{}) (interface{}, error)
	ModelToDb(ctx context.Context, model interface{}) (interface{}, error)
}

func BuildQueryById(id interface{}, modelType reflect.Type, idName string) (query map[string]interface{}) {
	columnName, _ := GetColumnName(modelType, idName)
	return map[string]interface{}{columnName: id}
}
func GetColumnName(modelType reflect.Type, jsonName string) (col string, colExist bool) {
	index := GetIndexByTag("json", jsonName, modelType)
	if index == -1 {
		return jsonName, false
	}
	field := modelType.Field(index)
	ormTag, ok2 := field.Tag.Lookup("gorm")
	if !ok2 {
		return "", true
	}
	if has := strings.Contains(ormTag, "column"); has {
		str1 := strings.Split(ormTag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == "column" {
					return str2[j+1], true
				}
			}
		}
	}
	return jsonName, false
}
func GetIndexByTag(tag, key string, modelType reflect.Type) (index int) {
	for i := 0; i < modelType.NumField(); i++ {
		f := modelType.Field(i)
		v := strings.Split(f.Tag.Get(tag), ",")[0]
		if v == key {
			return i
		}
	}
	return -1
}
func MakeJsonColumnMap(modelType reflect.Type) map[string]string {
	numField := modelType.NumField()
	mapJsonColumn := make(map[string]string)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		column, ok := findTag(ormTag, "column")
		if ok {
			tag1, ok1 := field.Tag.Lookup("json")
			tagJsons := strings.Split(tag1, ",")
			if ok1 && len(tagJsons) > 0 {
				mapJsonColumn[tagJsons[0]] = column
			}
		}
	}
	return mapJsonColumn
}
func FindFieldIndex(modelType reflect.Type, fieldName string) int {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		if field.Name == fieldName {
			return i
		}
	}
	return -1
}
func GetFieldByIndex(ModelType reflect.Type, index int) (json string, col string, colExist bool) {
	fields := ModelType.Field(index)
	tag, _ := fields.Tag.Lookup("gorm")

	if has := strings.Contains(tag, "column"); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		json = fields.Name
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == "column" {
					jTag, jOk := fields.Tag.Lookup("json")
					if jOk {
						tagJsons := strings.Split(jTag, ",")
						json = tagJsons[0]
					}
					return json, str2[j+1], true
				}
			}
		}
	}
	return "", "", false
}
func JSONToColumns(model map[string]interface{}, m map[string]string) map[string]interface{} {
	if model == nil || m == nil {
		return model
	}
	r := make(map[string]interface{})
	for k, v := range model {
		col, ok := m[k]
		if ok {
			r[col] = v
		}
	}
	return r
}
func GetWritableColumns(fields map[string]*FieldDB, jsonColumnMap map[string]string) map[string]string {
	m := jsonColumnMap
	for k, v := range jsonColumnMap {
		for _, db := range fields {
			if db.Column == v {
				if db.Update == false && db.Key == false {
					delete(m, k)
				}
			}
		}
	}
	return m
}

func Contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
func BuildToPatch(table string, model map[string]interface{}, keyColumns []string) (string, []interface{}) {
	return BuildToPatchWithVersion(table, model, keyColumns, "")
}
func BuildToPatchWithVersion(table string, model map[string]interface{}, keyColumns []string, version string) (string, []interface{}) { //version column name db
	values := make([]string, 0)
	where := make([]string, 0)
	args := make([]interface{}, 0)
	i := 1
	for col, v := range model {
		if !Contains(keyColumns, col) && col != version {
			if v == nil {
				values = append(values, col+"=null")
			} else {
				v2, ok2 := GetDBValue(v, -1)
				if ok2 {
					values = append(values, col+"="+v2)
				} else {
					values = append(values, col+"="+BuildParam(i))
					i = i + 1
					args = append(args, v)
				}
			}
		}
	}
	for _, col := range keyColumns {
		v0, ok0 := model[col]
		if ok0 {
			v, ok1 := GetDBValue(v0, -1)
			if ok1 {
				where = append(where, col+"="+v)
			} else {
				where = append(where, col+"="+BuildParam(i))
				i = i + 1
				args = append(args, v0)
			}
		}
	}
	if len(version) > 0 {
		v0, ok0 := model[version]
		if ok0 {
			switch v4 := v0.(type) {
			case int:
				values = append(values, version+"="+strconv.Itoa(v4+1))
				where = append(where, version+"="+strconv.Itoa(v4))
			case int32:
				v5 := int64(v4)
				values = append(values, version+"="+strconv.FormatInt(v5+1, 10))
				where = append(where, version+"="+strconv.FormatInt(v5, 10))
			case int64:
				values = append(values, version+"="+strconv.FormatInt(v4+1, 10))
				where = append(where, version+"="+strconv.FormatInt(v4, 10))
			}
		}
	}
	query := fmt.Sprintf("update %v set %v where %v", table, strings.Join(values, ","), strings.Join(where, " and "))
	return query, args
}
