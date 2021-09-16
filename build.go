package cassandra

import (
	"fmt"
	"reflect"
	"strings"
)

const IgnoreReadWrite = "-"

func BuildToInsert(table string, model interface{}, buildParam func(int) string) (string, []interface{}) {
	mapData, mapKey, columns, keys := BuildMapDataAndKeys(model, false)
	var cols []string
	var values []interface{}
	var params []string
	i := 1
	for _, columnName := range keys {
		if value, ok := mapKey[columnName]; ok {
			cols = append(cols, QuoteColumnName(columnName))
			v2b, ok2 := GetDBValue(value)
			if ok2 {
				params = append(params, v2b)
			} else {
				values = append(values, value)
				p := buildParam(i)
				params = append(params, p)
				i++
			}
		}
	}
	for _, columnName := range columns {
		if v1, ok := mapData[columnName]; ok {
			cols = append(cols, QuoteColumnName(columnName))
			v1b, ok1 := GetDBValue(v1)
			if ok1 {
				params = append(params, v1b)
			} else {
				values = append(values, v1)
				p := buildParam(i)
				params = append(params, p)
				i++
			}
		}
	}
	column := strings.Join(cols, ",")
	return fmt.Sprintf("insert into %v(%v)values(%v)", table, column, strings.Join(params, ",")), values
}

func BuildToInsertWithVersion(table string, model interface{}, versionIndex int, buildParam func(int) string) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version index not found")
	}

	var versionValue int64 = 1
	_, err := setValue(model, versionIndex, &versionValue)
	if err != nil {
		panic(err)
	}
	i := 1
	mapData, mapKey, columns, keys := BuildMapDataAndKeys(model, false)
	var cols []string
	var values []interface{}
	var params []string
	for _, columnName := range keys {
		if value, ok := mapKey[columnName]; ok {
			cols = append(cols, QuoteColumnName(columnName))
			v2b, ok2 := GetDBValue(value)
			if ok2 {
				params = append(params, v2b)
			} else {
				values = append(values, value)
				p := buildParam(i)
				params = append(params, p)
				i++
			}
		}
	}
	for _, columnName := range columns {
		if v1, ok := mapData[columnName]; ok {
			cols = append(cols, QuoteColumnName(columnName))
			v1b, ok1 := GetDBValue(v1)
			if ok1 {
				params = append(params, v1b)
			} else {
				values = append(values, v1)
				p := buildParam(i)
				params = append(params, p)
				i++
			}
		}
	}
	column := strings.Join(cols, ",")
	return fmt.Sprintf("insert into %v(%v)values(%v)", table, column, strings.Join(params, ",")), values
}
func BuildToUpdate(table string, model interface{}, buildParam func(int) string) (string, []interface{}) {
	mapData, mapKey, columns, keys := BuildMapDataAndKeys(model, true)
	var values []interface{}
	colSet := make([]string, 0)
	colQuery := make([]string, 0)
	colNumber := 1
	for _, colName := range columns {
		if v1, ok := mapData[colName]; ok {
			v3, ok3 := GetDBValue(v1)
			if ok3 {
				colSet = append(colSet, QuoteColumnName(colName)+"="+v3)
			} else {
				values = append(values, v1)
				colSet = append(colSet, QuoteColumnName(colName)+"="+buildParam(colNumber))
				colNumber++
			}
		} else {
			colSet = append(colSet, BuildParamWithNull(colName))
		}
	}
	for _, colName := range keys {
		if v2, ok := mapKey[colName]; ok {
			v3, ok3 := GetDBValue(v2)
			if ok3 {
				colQuery = append(colQuery, QuoteColumnName(colName)+"="+v3)
			} else {
				values = append(values, v2)
				colQuery = append(colQuery, QuoteColumnName(colName)+"="+buildParam(colNumber))
			}
			colNumber++
		}
	}
	queryWhere := strings.Join(colQuery, " and ")
	querySet := strings.Join(colSet, ",")
	query := fmt.Sprintf("update %v set %v where %v", table, querySet, queryWhere)
	return query, values
}
func BuildToUpdateWithVersion(table string, model interface{}, versionIndex int, buildParam func(int) string) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version's index not found")
	}
	valueOfModel := reflect.Indirect(reflect.ValueOf(model))
	currentVersion := reflect.Indirect(valueOfModel.Field(versionIndex)).Int()
	nextVersion := currentVersion + 1
	_, err := setValue(model, versionIndex, &nextVersion)
	if err != nil {
		panic(err)
	}

	mapData, mapKey, columns, keys := BuildMapDataAndKeys(model, true)
	versionColName, exist := GetColumnNameByIndex(valueOfModel.Type(), versionIndex)
	if !exist {
		panic("version's column not found")
	}
	mapKey[versionColName] = currentVersion

	var values []interface{}
	colSet := make([]string, 0)
	colQuery := make([]string, 0)
	colNumber := 1
	for _, colName := range columns {
		if v1, ok := mapData[colName]; ok {
			v3, ok3 := GetDBValue(v1)
			if ok3 {
				colSet = append(colSet, fmt.Sprintf("%v = "+v3, colName))
			} else {
				values = append(values, v1)
				colQuery = append(colQuery, QuoteColumnName(colName)+"="+buildParam(colNumber))
				colNumber++
			}
		} else {
			colSet = append(colSet, BuildParamWithNull(colName))
		}
	}
	for _, colName := range keys {
		if v2, ok := mapKey[colName]; ok {
			v3, ok3 := GetDBValue(v2)
			if ok3 {
				colQuery = append(colQuery, QuoteColumnName(colName)+"="+v3)
			} else {
				values = append(values, v2)
				colQuery = append(colQuery, QuoteColumnName(colName)+"="+buildParam(colNumber))
			}
			colNumber++
		}
	}
	queryWhere := strings.Join(colQuery, " and ")
	querySet := strings.Join(colSet, ",")
	query := fmt.Sprintf("update %v set %v where %v", table, querySet, queryWhere)
	return query, values
}

func BuildPatch(table string, model map[string]interface{}, mapJsonColum map[string]string, idTagJsonNames []string, idColumNames []string, buildParam func(int) string) (string, []interface{}) {
	scope := statement()
	// Append variables set column
	for key, _ := range model {
		if _, ok := Find(idTagJsonNames, key); !ok {
			if colName, ok2 := mapJsonColum[key]; ok2 {
				scope.Columns = append(scope.Columns, colName)
				scope.Values = append(scope.Values, model[key])
			}
		}
	}
	// Append variables where
	for i, key := range idTagJsonNames {
		scope.Values = append(scope.Values, model[key])
		scope.Keys = append(scope.Keys, idColumNames[i])
	}
	var value []interface{}

	n := len(scope.Columns)
	sets, val1, err1 := BuildSqlParametersAndValues(scope.Columns, scope.Values, &n, 0, ", ", buildParam)
	if err1 != nil {
		return "", nil
	}
	value = append(value, val1...)
	columnsKeys := len(scope.Keys)
	where, val2, err2 := BuildSqlParametersAndValues(scope.Keys, scope.Values, &columnsKeys, n, " and ", buildParam)
	if err2 != nil {
		return "", nil
	}
	value = append(value, val2...)
	query := fmt.Sprintf("update %s set %s where %s",
		table,
		sets,
		where,
	)
	return query, value
}

func BuildPatchWithVersion(table string, model map[string]interface{}, mapJsonColum map[string]string, idTagJsonNames []string, idColumNames []string, buildParam func(int) string, versionIndex int, versionJsonName, versionColName string) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version's index not found")
	}

	currentVersion, ok := model[versionJsonName]
	if !ok {
		panic("version field not found")
	}
	nextVersion := currentVersion.(int64) + 1
	model[versionJsonName] = nextVersion

	scope := statement()
	var value []interface{}
	// Append variables set column
	for key, _ := range model {
		if _, ok := Find(idTagJsonNames, key); !ok {
			if columName, ok2 := mapJsonColum[key]; ok2 {
				scope.Columns = append(scope.Columns, columName)
				scope.Values = append(scope.Values, model[key])
			}
		}
	}
	// Append variables where
	for i, key := range idTagJsonNames {
		scope.Values = append(scope.Values, model[key])
		scope.Keys = append(scope.Keys, idColumNames[i])
	}
	scope.Values = append(scope.Values, currentVersion)
	scope.Keys = append(scope.Keys, versionColName)

	n := len(scope.Columns)
	sets, setVal, err1 := BuildSqlParametersAndValues(scope.Columns, scope.Values, &n, 0, ", ", buildParam)
	if err1 != nil {
		return "", nil
	}
	value = append(value, setVal...)
	numKeys := len(scope.Keys)
	where, whereVal, err2 := BuildSqlParametersAndValues(scope.Keys, scope.Values, &numKeys, n, " and ", buildParam)
	if err2 != nil {
		return "", nil
	}
	value = append(value, whereVal...)
	query := fmt.Sprintf("update %s set %s where %s",
		table,
		sets,
		where,
	)
	return query, value
}

func BuildToDelete(table string, ids map[string]interface{}, buildParam func(int) string) (string, []interface{}) {
	var values []interface{}
	var queryArr []string
	i := 1
	for key, value := range ids {
		queryArr = append(queryArr, fmt.Sprintf("%v = %v", QuoteColumnName(key), buildParam(i)))
		values = append(values, value)
		i++
	}
	q := strings.Join(queryArr, " and ")
	return fmt.Sprintf("delete from %v where %v", table, q), values
}
