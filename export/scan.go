package export

import (
	"errors"
	"github.com/apache/cassandra-gocql-driver"
	"reflect"
	"strings"
)

func GetColumnIndexes(modelType reflect.Type) (map[string]int, error) {
	ma := make(map[string]int, 0)
	if modelType.Kind() != reflect.Struct {
		return ma, errors.New("bad type")
	}
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		column, ok := FindTag(ormTag, "column")
		column = strings.ToLower(column)
		if ok {
			ma[column] = i
		}
	}
	return ma, nil
}
func FindTag(tag string, key string) (string, bool) {
	if has := strings.Contains(tag, key); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == key {
					return str2[j+1], true
				}
			}
		}
	}
	return "", false
}
func StructScan(s interface{}, columns []string, fieldsIndex map[string]int, indexIgnore int) (r []interface{}) {
	if s != nil {
		modelType := reflect.TypeOf(s).Elem()
		maps := reflect.Indirect(reflect.ValueOf(s))
		if columns == nil {
			for i := 0; i < maps.NumField(); i++ {
				r = append(r, maps.Field(i).Addr().Interface())
			}
			return
		}
		for i, columnsName := range columns {
			if i == indexIgnore {
				continue
			}
			var index int
			var ok bool
			var valueField reflect.Value
			if fieldsIndex == nil {
				if _, ok = modelType.FieldByName(columnsName); !ok {
					var t interface{}
					r = append(r, &t)
					continue
				}
				valueField = maps.FieldByName(columnsName)
			} else {
				if index, ok = fieldsIndex[columnsName]; !ok {
					var t interface{}
					r = append(r, &t)
					continue
				}
				valueField = maps.Field(index)
			}
			x := valueField.Addr().Interface()
			r = append(r, x)
		}
	}
	return
}
func GetColumns(cols []gocql.ColumnInfo) []string {
	c2 := make([]string, 0)
	if cols == nil {
		return c2
	}
	for _, c := range cols {
		s := strings.ToLower(c.Name)
		c2 = append(c2, s)
	}
	return c2
}
