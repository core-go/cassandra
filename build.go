package cassandra

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const IgnoreReadWrite = "-"

func BuildToInsert(table string, model interface{}, options ...*Schema) (string, []interface{}) {
	return BuildToInsertWithVersion(table, model, -1, false, options...)
}
func BuildToSave(table string, model interface{}, orUpdate bool, options ...*Schema) (string, []interface{}) {
	return BuildToInsertWithVersion(table, model, -1, orUpdate, options...)
}
func BuildToInsertWithVersion(table string, model interface{}, versionIndex int, orUpdate bool, options ...*Schema) (string, []interface{}) {
	buildParam := BuildParam
	modelType := reflect.TypeOf(model)
	var cols []string
	var schema map[string]FieldDB
	if len(options) > 0 {
		cols = options[0].Columns
		schema = options[0].Fields
	} else {
		cols, _, schema = MakeSchema(modelType)
	}
	mv := reflect.ValueOf(model)
	if mv.Kind() == reflect.Ptr {
		mv = mv.Elem()
	}
	values := make([]string, 0)
	args := make([]interface{}, 0)
	icols := make([]string, 0)
	i := 1
	for _, col := range cols {
		fdb := schema[col]
		if fdb.Index == versionIndex {
			icols = append(icols, col)
			values = append(values, "1")
		} else {
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
			if fdb.Insert {
				if isNil {
					if orUpdate {
						icols = append(icols, fdb.Column)
						values = append(values, "null")
					}
				} else {
					icols = append(icols, fdb.Column)
					v, ok := GetDBValue(fieldValue)
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
	}
	return fmt.Sprintf("insert into %v(%v) values (%v)", table, strings.Join(icols, ","), strings.Join(values, ",")), args
}
func BuildToUpdate(table string, model interface{}, options ...*Schema) (string, []interface{}) {
	return BuildToUpdateWithVersion(table, model, -1, options...)
}
func BuildToUpdateWithVersion(table string, model interface{}, versionIndex int, options ...*Schema) (string, []interface{}) {
	buildParam := BuildParam
	var cols, keys []string
	var schema map[string]FieldDB
	modelType := reflect.TypeOf(model)
	if len(options) > 0 {
		m := options[0]
		cols = m.Columns
		keys = m.Keys
		schema = m.Fields
	} else {
		cols, keys, schema = MakeSchema(modelType)
	}
	mv := reflect.ValueOf(model)
	if mv.Kind() == reflect.Ptr {
		mv = mv.Elem()
	}
	values := make([]string, 0)
	where := make([]string, 0)
	args := make([]interface{}, 0)
	vw := ""
	i := 1
	for _, col := range cols {
		fdb := schema[col]
		if fdb.Index == versionIndex {
			valueOfModel := reflect.Indirect(reflect.ValueOf(model))
			currentVersion := reflect.Indirect(valueOfModel.Field(versionIndex)).Int()
			nv := currentVersion + 1
			values = append(values, col+"="+strconv.FormatInt(nv, 10))
			vw = col + "=" + strconv.FormatInt(currentVersion, 10)
		} else if !fdb.Key && fdb.Update {
			//f := reflect.Indirect(reflect.ValueOf(model))
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
				values = append(values, col+"=null")
			} else {
				v, ok := GetDBValue(fieldValue)
				if ok {
					values = append(values, col+"="+v)
				} else {
					values = append(values, col+"="+buildParam(i))
					i = i + 1
					args = append(args, fieldValue)
				}
			}
		}
	}
	for _, col := range keys {
		fdb := schema[col]
		f := mv.Field(fdb.Index)
		fieldValue := f.Interface()
		if f.Kind() == reflect.Ptr {
			if !reflect.ValueOf(fieldValue).IsNil() {
				fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
			}
		}
		v, ok := GetDBValue(fieldValue)
		if ok {
			where = append(where, col+"="+v)
		} else {
			where = append(where, col+"="+buildParam(i))
			i = i + 1
			args = append(args, fieldValue)
		}
	}
	if len(vw) > 0 {
		where = append(where, vw)
	}
	query := fmt.Sprintf("update %v set %v where %v", table, strings.Join(values, ","), strings.Join(where, " and "))
	return query, args
}
func BuildToDelete(table string, ids map[string]interface{}) (string, []interface{}) {
	var values []interface{}
	var queryArr []string
	i := 1
	for col, value := range ids {
		queryArr = append(queryArr, col + "=?")
		values = append(values, value)
		i++
	}
	q := strings.Join(queryArr, " and ")
	return fmt.Sprintf("delete from %v where %v", table, q), values
}
