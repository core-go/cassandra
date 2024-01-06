package cassandra

import (
	"fmt"
	"reflect"
	"strings"
)

func InterfaceSlice(slice interface{}) ([]interface{}, error) {
	s := reflect.Indirect(reflect.ValueOf(slice))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("InterfaceSlice() given a non-slice type")
	}
	ret := make([]interface{}, s.Len())

	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}
	return ret, nil
}
func ToArrayIndex(value reflect.Value, indices []int) []int {
	for i := 0; i < value.Len(); i++ {
		indices = append(indices, i)
	}
	return indices
}
func BuildToInsertBatch(table string, models interface{}, options...*Schema) ([]Statement, error) {
	return BuildToInsertBatchWithVersion(table, models, -1, false, options...)
}
func BuildToInsertOrUpdateBatch(table string, models interface{}, orUpdate bool, options...*Schema) ([]Statement, error) {
	return BuildToInsertBatchWithVersion(table, models, -1, orUpdate, options...)
}
func BuildToInsertBatchWithVersion(table string, models interface{}, versionIndex int, orUpdate bool, options...*Schema) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() <= 0 {
		return nil, nil
	}
	var strt *Schema
	if len(options) > 0 {
		strt = options[0]
	} else {
		first := s.Index(0).Interface()
		modelType := reflect.TypeOf(first)
		strt = CreateSchema(modelType)
	}
	slen := s.Len()
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		// mv := reflect.ValueOf(model)
		query, args := BuildToInsertWithVersion(table, model, versionIndex, orUpdate, strt)
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
func BuildToUpdateBatch(table string, models interface{}, options ...*Schema) ([]Statement, error) {
	return BuildToUpdateBatchWithVersion(table, models, -1, options...)
}
func BuildToUpdateBatchWithVersion(table string, models interface{}, versionIndex int, options ...*Schema) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	slen := s.Len()
	if slen <= 0 {
		return nil, nil
	}
	var strt *Schema
	if len(options) > 0 {
		strt = options[0]
	} else {
		first := s.Index(0).Interface()
		modelType := reflect.TypeOf(first)
		strt = CreateSchema(modelType)
	}
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		// mv := reflect.ValueOf(model)
		query, args := BuildToUpdateWithVersion(table, model, versionIndex, strt)
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
func BuildToSaveBatch(table string, models interface{}, options ...*Schema) (string, []interface{}, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return "", nil, fmt.Errorf("models must be a slice")
	}
	slen := s.Len()
	if slen <= 0 {
		return "", nil, nil
	}
	buildParam := BuildParam
	var cols []*FieldDB
	// var schema map[string]FieldDB
	if len(options) > 0 && options[0] != nil {
		cols = options[0].Columns
		// schema = options[0].Fields
	} else {
		first := s.Index(0).Interface()
		modelType := reflect.TypeOf(first)
		m := CreateSchema(modelType)
		cols = m.Columns
	}
	placeholders := make([]string, 0)
	args := make([]interface{}, 0)
	i := 1
	icols := make([]string, 0)
	for _, fdb := range cols {
		if fdb.Insert {
			icols = append(icols, fdb.Column)
		}
	}
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		mv := reflect.ValueOf(model)
		values := make([]string, 0)
		for _, fdb := range cols {
			if fdb.Insert {
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if isNil {
					icols = append(icols, fdb.Column)
					values = append(values, "null")
				} else {
					v, ok := GetDBValue(fieldValue, fdb.Scale)
					if ok {
						values = append(values, v)
					} else {
						values = append(values, buildParam(i))
						i = i + 1
						args = append(args, fieldValue)
					}
				}
			}
		}
		x := "(" + strings.Join(values, ",") + ")"
		placeholders = append(placeholders, x)
	}
	query := fmt.Sprintf(fmt.Sprintf("insert into %s (%s) values %s",
		table,
		strings.Join(icols, ","),
		strings.Join(placeholders, ","),
	))
	return query, args, nil
}
