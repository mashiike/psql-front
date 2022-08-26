package origin

import (
	"fmt"
	"strconv"
	"strings"

	psqlfront "github.com/mashiike/psql-front"
	"github.com/samber/lo"
)

type ColumnConfig struct {
	Name        string `yaml:"name,omitempty"`
	DataType    string `yaml:"data_type,omitempty"`
	DataLength  *int   `yaml:"length,omitempty"`
	Contraint   string `yaml:"contraint,omitempty"`
	ColumnIndex *int   `yaml:"column_index,omitempty"`
}

type ColumnConfigs []*ColumnConfig

func (cfgs ColumnConfigs) Restrict() error {
	for j, column := range cfgs {
		if column.Name == "" {
			return fmt.Errorf("column[%d]: name is required", j)
		}
		if column.DataType == "" {
			column.DataType = "TEXT"
		}
	}
	return nil
}

func (cfgs ColumnConfigs) ToColumns() []*psqlfront.Column {
	return lo.Map(cfgs, func(column *ColumnConfig, _ int) *psqlfront.Column {
		return &psqlfront.Column{
			Name:      column.Name,
			DataType:  column.DataType,
			Length:    column.DataLength,
			Contraint: column.Contraint,
		}
	})
}

func (cfgs ColumnConfigs) ToRows(records [][]string, ignoreLines int) [][]interface{} {
	if ignoreLines > 0 {
		if ignoreLines >= len(records) {
			return nil
		}
		records = records[ignoreLines:]
	}
	return lo.Map(records, func(record []string, _ int) []interface{} {

		row := make([]interface{}, 0, len(cfgs))
		for i, c := range cfgs {
			if c.ColumnIndex != nil {
				if *c.ColumnIndex < len(record) {
					row = append(row, toDBValue(c.DataType, c.Contraint, record[*c.ColumnIndex]))
				} else {
					row = append(row, nil)
				}
				continue
			}
			if i < len(record) {
				row = append(row, toDBValue(c.DataType, c.Contraint, record[i]))
			} else {
				row = append(row, nil)
			}
		}
		return row
	})
}

func toDBValue(dataType string, constraint string, value string) interface{} {
	if value == "" {
		if !strings.Contains(strings.ToUpper(constraint), "NOT NULL") {
			return nil
		}
	}
	switch strings.ToUpper(dataType) {
	case "BIGINT", "INTEGER":
		if v, err := strconv.ParseInt(value, 10, 64); err == nil {
			return v
		}
		return value
	default:
		return value
	}
}
