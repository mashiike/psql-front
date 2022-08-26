package origin

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mattn/go-encoding"
	"github.com/saintfish/chardet"
	"github.com/samber/lo"
)

func ConvertTextEncoding(reader io.Reader, textEncoding *string) io.Reader {
	br := bufio.NewReader(reader)

	var textEncodings []string
	if textEncoding != nil {
		textEncodings = []string{*textEncoding}
	} else {
		det := chardet.NewTextDetector()
		if data, err := br.Peek(2048); err == nil || err == io.EOF {
			if res, err := det.DetectAll(data); err == nil {
				textEncodings = append(textEncodings, lo.Map(res, func(r chardet.Result, _ int) string {
					return r.Charset
				})...)
			}
		}
	}
	var r io.Reader = br
	if len(textEncodings) > 0 {
		for _, textEncoding := range textEncodings {
			if enc := encoding.GetEncoding(textEncoding); enc != nil {
				r = enc.NewDecoder().Reader(br)
				break
			}
		}
	}
	return r
}

func PerformSchemaInference(rows [][]string, ignoreLines int) (ColumnConfigs, error) {
	if len(rows) <= ignoreLines {
		return nil, errors.New("data not found")
	}
	columnNames := make(map[string]int)
	if ignoreLines == 1 {
		duplicationCount := make(map[string]int)
		duplicationCount["anonymous_field"] = 1
		for i, header := range rows[0] {
			var columnName string
			if canUseTableName(header) {
				columnName = header
			} else {
				columnName = "anonymous_field"
			}
			c, ok := duplicationCount[columnName]
			if ok {
				columnNames[fmt.Sprintf("%s%d", columnName, c)] = i
			} else {
				columnNames[columnName] = i
			}
			duplicationCount[columnName] = c + 1
		}
		rows = rows[1:]
	} else {
		for i := range rows[0] {
			columnNames[fmt.Sprintf("field%d", i+1)] = i
		}
	}
	columns := make(ColumnConfigs, 0, len(columnNames))
	for name, index := range columnNames {
		_index := index
		column := &ColumnConfig{
			Name:        name,
			ColumnIndex: &_index,
		}
		data := make([]string, 0, len(rows))
		for _, row := range rows {
			if index < len(row) {
				data = append(data, row[index])
			}
		}
		column.DataType, column.DataLength, column.Contraint = detectTypeInfo(data)
		columns = append(columns, column)
	}
	sort.Slice(columns, func(i, j int) bool {
		return *columns[i].ColumnIndex < *columns[j].ColumnIndex
	})
	return columns, nil
}

var canUseTableName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`).MatchString

var (
	dateLayouts      = []string{"2006-01-02", "2006/01/01"}
	timestampLayouts = []string{"2006-01-02T15:04:05", "2006/01/02T15:04:05", "2006-01-02T15:04", "2006/01/02T15:04", "2006-01-02 15:04:05", "2006/01/02 15:04:05", "2006-01-02 15:04", "2006/01/02 15:04"}
)

func asDate(str string) bool {
	for _, layout := range dateLayouts {
		if _, err := time.Parse(layout, str); err == nil {
			return true
		}
	}
	return asTimestamp(str)
}

func asTimestamp(str string) bool {
	for _, layout := range timestampLayouts {
		if _, err := time.Parse(layout, str); err == nil {
			return true
		}
	}
	return false
}

const limitMaxLength = 65535

func detectTypeInfo(data []string) (string, *int, string) {
	maxLength := 64
	dataTypeCandidates := map[string]bool{"VARCHAR": true, "DATE": true, "TIMESTAMP": true, "BOOLEAN": true, "BIGINT": true, "FLOAT": true}
	for index, d := range data {
		if len(dataTypeCandidates) <= 1 && maxLength == limitMaxLength {
			return "VARCHAR", &maxLength, ""
		}
		if d == "" {
			continue
		}
		if index >= 10000 {
			break
		}
		for c := range dataTypeCandidates {
			switch c {
			case "VARCHAR":
				if len(d) > maxLength {
					maxLength *= 2
				}
				if maxLength >= limitMaxLength {
					maxLength = limitMaxLength
				}
			case "DATE":
				if !asDate(d) {
					delete(dataTypeCandidates, "DATE")
				}
			case "TIMESTAMP":
				if !asTimestamp(d) {
					delete(dataTypeCandidates, "TIMESTAMP")
				}
			case "BOOLEAN":
				s := strings.TrimSpace(d)
				if !strings.EqualFold(s, "true") && !strings.EqualFold(s, "false") && s != "1" && s != "0" {
					delete(dataTypeCandidates, "BOOLEAN")
				}
			case "BIGINT":
				if _, err := strconv.ParseInt(d, 10, 64); err != nil {
					delete(dataTypeCandidates, "BIGINT")
				}
			case "FLOAT":
				if _, err := strconv.ParseFloat(d, 64); err != nil {
					delete(dataTypeCandidates, "FLOAT")
				}
			}
		}
	}
	if _, ok := dataTypeCandidates["FLOAT"]; ok {
		if _, ok := dataTypeCandidates["BIGINT"]; ok {
			return "BIGINT", nil, ""
		}
		return "FLOAT", nil, ""
	}
	if _, ok := dataTypeCandidates["BIGINT"]; ok {
		return "BIGINT", nil, ""
	}
	if _, ok := dataTypeCandidates["TIMESTAMP"]; ok {
		return "TIMESTAMP", nil, ""
	}
	if _, ok := dataTypeCandidates["BOOLEAN"]; ok {
		return "BOOLEAN", nil, ""
	}
	if _, ok := dataTypeCandidates["DATE"]; ok {
		return "DATE", nil, ""
	}
	return "VARCHAR", &maxLength, ""
}
