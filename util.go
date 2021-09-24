package cassandra

import (
	"reflect"
	"strconv"
	"strings"
)

type FieldDB struct {
	JSON   string
	Column string
	Field  string
	Index  int
	Key    bool
	Update bool
	Insert bool
	True   *string
	False  *string
}
type Schema struct {
	Keys    []string
	Columns []string
	Fields  map[string]FieldDB
}
func CreateSchema(modelType reflect.Type) *Schema {
	cols, keys, schema := MakeSchema(modelType)
	s := &Schema{Columns: cols, Keys: keys, Fields: schema}
	return s
}
func MakeSchema(modelType reflect.Type) ([]string, []string, map[string]FieldDB) {
	numField := 0
	if modelType.Kind() == reflect.Ptr {
		numField = modelType.Elem().NumField()
	} else {
		numField = modelType.NumField()
	}
	columns := make([]string, 0)
	keys := make([]string, 0)
	schema := make(map[string]FieldDB, 0)
	for idx := 0; idx < numField; idx++ {
		var field reflect.StructField
		if modelType.Kind() == reflect.Ptr {
			field = modelType.Elem().Field(idx)
		} else {
			field = modelType.Field(idx)
		}
		tag, _ := field.Tag.Lookup("gorm")
		if !strings.Contains(tag, IgnoreReadWrite) {
			update := !strings.Contains(tag, "update:false")
			insert := !strings.Contains(tag, "insert:false")
			if has := strings.Contains(tag, "column"); has {
				json := field.Name
				col := json
				str1 := strings.Split(tag, ";")
				num := len(str1)
				for i := 0; i < num; i++ {
					str2 := strings.Split(str1[i], ":")
					for j := 0; j < len(str2); j++ {
						if str2[j] == "column" {
							isKey := strings.Contains(tag, "primary_key")
							col = str2[j+1]
							columns = append(columns, col)
							if isKey {
								keys = append(keys, col)
							}

							jTag, jOk := field.Tag.Lookup("json")
							if jOk {
								tagJsons := strings.Split(jTag, ",")
								json = tagJsons[0]
							}
							f := FieldDB{
								JSON:   json,
								Column: col,
								Index:  idx,
								Key:    isKey,
								Update: update,
								Insert: insert,
							}
							tTag, tOk := field.Tag.Lookup("true")
							if tOk {
								f.True = &tTag
								fTag, fOk := field.Tag.Lookup("false")
								if fOk {
									f.False = &fTag
								}
							}
							schema[col] = f
						}
					}
				}
			}
		}
	}
	return columns, keys, schema
}
func GetDBValue(v interface{}) (string, bool) {
	switch v.(type) {
	case string:
		s0 := v.(string)
		if len(s0) == 0 {
			return "''", true
		}
		return "", false
	case bool:
		b0 := v.(bool)
		if b0 {
			return "true", true
		} else {
			return "false", true
		}
		return "", false
	case int:
		return strconv.Itoa(v.(int)), true
	case int64:
		return strconv.FormatInt(v.(int64), 10), true
	case int32:
		return strconv.FormatInt(int64(v.(int32)), 10), true
	default:
		return "", false
	}
}
