package cassandra

import (
	"encoding/hex"
	"reflect"
	"strings"

	"github.com/apache/cassandra-gocql-driver"
)

func QueryMap(ses *gocql.Session, transform func(s string) string, sql string, values ...interface{}) ([]map[string]interface{}, error) {
	q := ses.Query(sql, values...)
	list := make([]map[string]interface{}, 0)
	if q.Exec() != nil {
		return list, q.Exec()
	}
	iter := q.Iter()
	if transform == nil {
		for {
			row := make(map[string]interface{})
			if !iter.MapScan(row) {
				return list, nil
			} else {
				list = append(list, row)
			}
		}
	} else {
		rowData, err := iter.RowData()
		if err != nil {
			return list, err
		}
		var cols []string
		for _, col := range rowData.Columns {
			cols = append(cols, transform(col))
		}
		for {
			row := make(map[string]interface{})
			boolScan := ScanMap(row, iter, rowData, cols)
			if !boolScan {
				return list, nil
			} else {
				list = append(list, row)
			}
		}
	}
}
func ScanMap(m map[string]interface{}, iter *gocql.Iter, rowData gocql.RowData, newCols []string) bool {
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
				m[newCols[i]] = valCopy.Interface()
			} else {
				m[newCols[i]] = val
			}
		}
		return true
	}
	return false
}
func Query(ses *gocql.Session, fieldsIndex map[string]int, results interface{}, sql string, values ...interface{}) error {
	q := ses.Query(sql, values...)
	if q.Exec() != nil {
		return q.Exec()
	}
	return ScanIter(q.Iter(), results, fieldsIndex)
}
func QueryWithPage(ses *gocql.Session, fieldsIndex map[string]int, results interface{}, max int64, refId string, sql string, values ...interface{}) (string, error) {
	next, er0 := hex.DecodeString(refId)
	if er0 != nil {
		return "", er0
	}
	query := ses.Query(sql, values...).PageState(next).PageSize(int(max))
	if query.Exec() != nil {
		return "", query.Exec()
	}
	err := ScanIter(query.Iter(), results, fieldsIndex)
	if err != nil {
		return "", err
	}
	nextPageToken := hex.EncodeToString(query.Iter().PageState())
	return nextPageToken, nil
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
