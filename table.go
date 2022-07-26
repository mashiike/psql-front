package psqlfront

import (
	"errors"
	"fmt"
	"strings"
)

type Table struct {
	SchemaName string
	RelName    string

	Columns     []*Column
	Constraints []string
}

type Column struct {
	Name      string
	DataType  string
	Length    *int
	Contraint string
}

func (t *Table) String() string {
	return fmt.Sprintf(`"%s"."%s"`, t.SchemaName, t.RelName)
}

func (t *Table) GoString() string {
	return t.String()
}

func (t *Table) GenerateDDL() (string, error) {
	fields := make([]string, 0)
	if len(t.Columns) == 0 {
		return "", errors.New("columns is required")
	}
	for _, column := range t.Columns {
		if column.Length != nil && *column.Length > 0 {
			fields = append(fields, strings.Join([]string{column.Name, fmt.Sprintf("%s(%d)", column.DataType, *column.Length), column.Contraint}, " "))
		} else {
			fields = append(fields, strings.Join([]string{column.Name, column.DataType, column.Contraint}, " "))
		}
	}
	return fmt.Sprintf("CREATE TABLE %s (\n    %s\n);", t, strings.Join(fields, ",\n    ")), nil
}
