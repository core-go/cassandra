package cassandra

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"reflect"
	"strings"
)

func InitFields(modelType reflect.Type) (map[string]int, string, error) {
	fieldsIndex, err := GetColumnIndexes(modelType)
	if err != nil {
		return nil, "", err
	}
	fields := BuildFields(modelType)
	return fieldsIndex, fields, nil
}
type Loader struct {
	DB                *gocql.ClusterConfig
	BuildParam        func(i int) string
	Map               func(ctx context.Context, model interface{}) (interface{}, error)
	modelType         reflect.Type
	modelsType        reflect.Type
	keys              []string
	mapJsonColumnKeys map[string]string
	fieldsIndex       map[string]int
	table             string
	query             string
}

func NewLoader(db *gocql.ClusterConfig, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) (*Loader, error) {
	_, idNames := FindPrimaryKeys(modelType)
	mapJsonColumnKeys := MapJsonColumn(modelType)
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()

	fieldsIndex, er0 := GetColumnIndexes(modelType)
	if er0 != nil {
		return nil, er0
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	query := BuildQuery(tableName, modelType)
	return &Loader{DB: db, BuildParam: BuildParam, Map: mp, modelType: modelType, modelsType: modelsType, keys: idNames, mapJsonColumnKeys: mapJsonColumnKeys, fieldsIndex: fieldsIndex, table: tableName, query: query}, nil
}

func (s *Loader) Keys() []string {
	return s.keys
}

func (s *Loader) All(ctx context.Context) (interface{}, error) {
	result := reflect.New(s.modelsType).Interface()
	ses, err := s.DB.CreateSession()
	if err != nil {
		return nil, err
	}
	defer ses.Close()

	q := ses.Query(s.query)
	err = q.Exec()
	if err != nil {
		return nil, err
	}
	err = ScanIter(q.Iter(), result, s.fieldsIndex)
	if err == nil {
		if s.Map != nil {
			return MapModels(ctx, result, s.Map)
		}
	}
	return result, err
}

func (s *Loader) Load(ctx context.Context, id interface{}) (interface{}, error) {
	queryFindById, values := BuildFindById(s.query, s.BuildParam, id, s.mapJsonColumnKeys, s.keys)
	ses, err := s.DB.CreateSession()
	if err != nil {
		return nil, err
	}
	defer ses.Close()

	q := ses.Query(queryFindById, values...)
	err = q.Exec()
	if err != nil {
		return nil, err
	}
	arr, err := Scan(q.Iter(), s.modelType, s.fieldsIndex)
	if err != nil {
		return nil, err
	}
	if len(arr) > 0 {
		if s.Map != nil {
			_, er2 := s.Map(ctx, &arr[0])
			return &arr[0], er2
		}
		return &arr[0], nil
	} else {
		return nil, nil
	}
}

func (s *Loader) LoadAndDecode(ctx context.Context, id interface{}, result interface{}) (bool, error) {
	return s.Get(ctx, id, result)
}

func (s *Loader) Get(ctx context.Context, id interface{}, result interface{}) (bool, error) {
	queryFindById, values := BuildFindById(s.query, s.BuildParam, id, s.mapJsonColumnKeys, s.keys)
	ses, err := s.DB.CreateSession()
	if err != nil {
		return false, err
	}
	defer ses.Close()

	q := ses.Query(queryFindById, values...)
	err = q.Exec()
	if err != nil {
		return false, err
	}
	iter := q.Iter()
	columns := GetColumns(iter.Columns())
	r := StructScan(result, columns, s.fieldsIndex, -1)
	if !iter.Scan(r...) {
		return false, nil
	} else {
		if s.Map != nil {
			_, er2 := s.Map(ctx, result)
			return true, er2
		}
		return true, nil
	}
}

func (s *Loader) Exist(ctx context.Context, id interface{}) (bool, error) {
	v, err := s.Load(ctx, id)
	if err != nil {
		return false, err
	}
	ok := IsNil(v)
	return ok, nil
}

func FindPrimaryKeys(modelType reflect.Type) ([]string, []string) {
	numField := modelType.NumField()
	var idColumnFields []string
	var idJsons []string
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				k, ok := findTag(ormTag, "column")
				if ok {
					idColumnFields = append(idColumnFields, k)
					tag1, ok1 := field.Tag.Lookup("json")
					tagJsons := strings.Split(tag1, ",")
					if ok1 && len(tagJsons) > 0 {
						idJsons = append(idJsons, tagJsons[0])
					}
				}
			}
		}
	}
	return idColumnFields, idJsons
}
func findTag(tag string, key string) (string, bool) {
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
func MapJsonColumn(modelType reflect.Type) map[string]string {
	numField := modelType.NumField()
	columnNameKeys := make(map[string]string)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				if has := strings.Contains(ormTag, "column"); has {
					str1 := strings.Split(ormTag, ";")
					num := len(str1)
					for i := 0; i < num; i++ {
						str2 := strings.Split(str1[i], ":")
						for j := 0; j < len(str2); j++ {
							if str2[j] == "column" {
								tagj, ok1 := field.Tag.Lookup("json")
								t := strings.Split(tagj, ",")
								if ok1 && len(t) > 0 {
									json := t[0]
									columnNameKeys[json] = str2[j+1]
								}
							}
						}
					}
				}
			}
		}
	}
	return columnNameKeys
}

func BuildSelectAllQuery(table string) string {
	return fmt.Sprintf("select * from %v", table)
}

func BuildFindById(query string, buildParam func(i int) string, id interface{}, mapJsonColumnKeys map[string]string, keys []string) (string, []interface{}) {
	var where = ""
	var values []interface{}
	if len(keys) == 1 {
		where = fmt.Sprintf("where %s = %s", mapJsonColumnKeys[keys[0]], buildParam(1))
		values = append(values, id)
	} else {
		conditions := make([]string, 0)
		if ids, ok := id.(map[string]interface{}); ok {
			j := 0
			for _, keyJson := range keys {
				columnName := mapJsonColumnKeys[keyJson]
				if idk, ok1 := ids[keyJson]; ok1 {
					conditions = append(conditions, fmt.Sprintf("%s = %s", columnName, buildParam(j)))
					values = append(values, idk)
					j++
				}
			}
			where = "where " + strings.Join(conditions, " and ")
		}
	}
	return fmt.Sprintf("%v %v", query, where), values
}
func IsNil(i interface{}) bool {
	if i == nil {
		return true
	}
	switch reflect.TypeOf(i).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		return reflect.ValueOf(i).IsNil()
	}
	return false
}
