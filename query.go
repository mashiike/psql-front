package psqlfront

import (
	"bytes"
	"fmt"
	"log"
	"strings"

	json "github.com/goccy/go-json"
	pgquery "github.com/pganalyze/pg_query_go/v2"
	"github.com/samber/lo"
)

func AnalyzeQuery(query string) ([]*Table, error) {
	tree, err := pgquery.ParseToJSON(query)
	if err != nil {
		return nil, fmt.Errorf("parse query: %w", err)
	}
	var dst bytes.Buffer
	json.Indent(&dst, []byte(tree), "", "  ")
	log.Printf("[debug] query: %s", query)
	log.Printf("[debug] structure: \n%s", dst.String())
	var obj interface{}
	if err := json.Unmarshal([]byte(tree), &obj); err != nil {
		return nil, err
	}
	stmts, err := findJSONValues[map[string]interface{}](obj, "stmt")
	if err != nil {
		return nil, err
	}
	var isTablesParseTarget bool
	for _, stmt := range stmts {
		if _, ok := stmt["SelectStmt"]; ok {
			isTablesParseTarget = true
		} else if _, ok := stmt["DeclareCursorStmt"]; ok {
			isTablesParseTarget = true
		}
	}
	if !isTablesParseTarget {
		return []*Table{}, nil
	}
	fromClauses, err := findJSONValues[[]interface{}](obj, "fromClause")
	if err != nil {
		return nil, err
	}
	ctes, err := findJSONValues[string](obj, "ctename")
	if err != nil {
		return nil, err
	}
	tables := make([]*Table, 0, len(fromClauses))
	var refClass, refNamespace bool
	for _, fromClause := range fromClauses {
		rangeVars, err := findJSONValues[map[string]interface{}](fromClause, "RangeVar")
		if err != nil {
			return nil, err
		}
		for _, rangeVar := range rangeVars {
			relname, ok := rangeVar["relname"].(string)
			if !ok {
				continue
			}
			if lo.Contains(ctes, relname) {
				continue
			}
			table := &Table{
				RelName: relname,
			}
			if schemaname, ok := rangeVar["schemaname"].(string); ok {
				table.SchemaName = schemaname
			} else if strings.HasPrefix(relname, "pg_") {
				table.SchemaName = "pg_catalog"
			} else {
				table.SchemaName = "public"
			}
			tables = append(tables, table)
			if strings.EqualFold(relname, "pg_namespace") {
				refNamespace = true
			}
			if strings.EqualFold(relname, "pg_class") {
				refClass = true
			}
		}
	}
	if !refClass || !refNamespace {
		return tables, nil
	}
	//extra check
	whereClauses, err := findJSONValues[map[string]interface{}](obj, "whereClause")
	if err != nil {
		return nil, err
	}
	if len(whereClauses) == 0 {
		return tables, nil
	}
	whereClause, ok := whereClauses[0]["BoolExpr"].(map[string]interface{})
	if !ok {
		return tables, nil
	}
	op, ok := whereClause["boolop"]
	if !ok {
		return tables, nil
	}
	if op != "AND_EXPR" {
		return tables, nil
	}
	args, ok := whereClause["args"].([]interface{})
	if !ok {
		return tables, nil
	}
	var schemaName, relName string
	log.Println("[debug] extruct schema and relname condition")
	for i, arg := range args {
		var refNspname, refRelname bool
		columnRefs, err := findJSONValues[map[string]interface{}](arg, "ColumnRef")
		if err != nil {
			return nil, err
		}
		log.Printf("[debug] args[%d] %d columnRef ", i, len(columnRefs))
		for _, columnRef := range columnRefs {
			strs, err := findJSONValues[string](columnRef, "str")
			if err != nil {
				return nil, err
			}
			log.Printf("[debug] columnRef strs %v", strs)
			if lo.ContainsBy(strs, func(s string) bool { return strings.EqualFold(s, "nspname") }) {
				refNspname = true
				break
			}
			if lo.ContainsBy(strs, func(s string) bool { return strings.EqualFold(s, "relname") }) {
				refRelname = true
				break
			}
		}
		if !refNspname && !refRelname {
			log.Printf("[debug] args[%d] not shema or relname condition ", i)
			continue
		}
		aConsts, err := findJSONValues[map[string]interface{}](arg, "A_Const")
		if err != nil {
			return nil, err
		}
		for _, aConst := range aConsts {
			strs, err := findJSONValues[string](aConst, "str")
			if err != nil {
				return nil, err
			}
			if len(strs) == 0 {
				continue
			}
			if refNspname {
				schemaName = strs[0]
				log.Printf("[debug] args[%d] nspname=%s", i, schemaName)
				break
			}
			if refRelname {
				relName = strs[0]
				log.Printf("[debug] args[%d] relname=%s", i, relName)
				break
			}
		}
	}
	if schemaName != "" && relName != "" {
		tables = append(tables, &Table{
			SchemaName: schemaName,
			RelName:    relName,
		})
	}
	return tables, nil
}

func findJSONValues[T any](obj interface{}, key string) ([]T, error) {
	return findJSONValuesHelper(obj, "", key, []T{})
}

func findJSONValuesHelper[T any](obj interface{}, path string, key string, list []T) ([]T, error) {
	if obj == nil {
		return list, nil
	}
	if a, ok := obj.([]interface{}); ok {
		for i, item := range a {
			l, err := findJSONValuesHelper(item, fmt.Sprintf("%s[%d]", path, i), key, []T{})
			if err != nil {
				return nil, err
			}
			list = append(list, l...)
		}
		return list, nil
	}
	if o, ok := obj.(map[string]interface{}); ok {
		for k, value := range o {
			if k == key {
				if v, ok := value.(T); ok {
					list = append(list, v)
				}
				continue
			}
			l, err := findJSONValuesHelper(value, fmt.Sprintf("%s.%s", path, k), key, []T{})
			if err != nil {
				return nil, err
			}
			list = append(list, l...)
		}
	}
	return list, nil
}
