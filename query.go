package cassandra

import (
	"encoding/hex"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
)

func QueryMap(ses *gocql.Session, sql string, values ...interface{}) ([]map[string]interface{}, error) {
	q := ses.Query(sql, values...)
	list := make([]map[string]interface{}, 0)
	if q.Exec() != nil {
		return list, q.Exec()
	}
	iter := q.Iter()
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			return list, nil
		} else {
			list = append(list, row)
		}
	}
}
func Query(ses *gocql.Session, fieldsIndex map[string]int, results interface{}, sql string, values ...interface{}) error {
	q := ses.Query(sql, values...)
	if q.Exec() != nil {
		return q.Exec()
	}
	return ScanIter(q.Iter(), results, fieldsIndex)
}
func QueryWithPage(ses *gocql.Session, fieldsIndex map[string]int, results interface{}, sql string, values []interface{}, max int, options ...string) (string, error) {
	nextPageToken := ""
	if len(options) > 0 && len(options[0]) > 0 {
		nextPageToken = options[0]
	}
	next, er0 := hex.DecodeString(nextPageToken)
	if er0 != nil {
		return "", er0
	}
	query := ses.Query(sql, values...).PageState(next).PageSize(max)
	if query.Exec() != nil {
		return "", query.Exec()
	}
	err := ScanIter(query.Iter(), results)
	if err != nil {
		return "", err
	}
	nextPageToken = hex.EncodeToString(query.Iter().PageState())
	return nextPageToken, nil
}

func QueryMapWithColumn(ses *gocql.Session, sql string, values ...interface{}) ([]map[string]interface{}, error) {
	q := ses.Query(sql, values...)
	list := make([]map[string]interface{}, 0)
	if q.Exec() != nil {
		return list, q.Exec()
	}
	iter := q.Iter()
	rowData, _ := iter.RowData()
	var columnCamel []string
	for _, col := range rowData.Columns {
		columnCamel = append(columnCamel, ToCamelCase(col))
	}
	for {
		row := make(map[string]interface{})
		boolScan := MapScanWithColumn(row, iter, columnCamel)
		if !boolScan {
			return list, nil
		} else {
			list = append(list, row)
		}
	}
}

func MapScanWithColumn(m map[string]interface{}, iter *gocql.Iter, columnCamel []string) bool {
	rowData, _ := iter.RowData()

	for i, col := range rowData.Columns {
		if dest, ok := m[col]; ok {
			rowData.Values[i] = dest
		}
	}

	if iter.Scan(rowData.Values...) {
		for i, _ := range rowData.Values {
			val := reflect.Indirect(reflect.ValueOf(rowData.Values[i])).Interface()
			if valVal := reflect.ValueOf(val); valVal.Kind() == reflect.Slice {
				valCopy := reflect.MakeSlice(valVal.Type(), valVal.Len(), valVal.Cap())
				reflect.Copy(valCopy, valVal)
				m[columnCamel[i]] = valCopy.Interface()
			} else {
				m[columnCamel[i]] = val
			}
		}
		return true
	}
	return false
}

func ToCamelCase(s string) string {
	s2 := strings.ToLower(s)
	s1 := string(s2[0])
	for i := 1; i < len(s); i++ {
		if string(s2[i-1]) == "_" {
			s1 = s1[:len(s1)-1]
			s1 += strings.ToUpper(string(s2[i]))
		} else {
			s1 += string(s2[i])
		}
	}
	return s1
}
