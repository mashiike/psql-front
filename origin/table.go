package origin

import (
	"context"
	"fmt"
	"log"

	psqlfront "github.com/mashiike/psql-front"
)

type BaseTableConfig struct {
	schema  string        `yaml:"-"`
	Name    string        `yaml:"name,omitempty"`
	Columns ColumnConfigs `yaml:"columns,omitempty"`
}

func (cfg *BaseTableConfig) Restrict(schema string) error {
	cfg.schema = schema
	if cfg.Name == "" {
		return fmt.Errorf("name is required")
	}
	if err := cfg.Columns.Restrict(); err != nil {
		return err
	}
	return nil
}

type Fetcher func(context.Context) ([][]string, error)

func (cfg *BaseTableConfig) FetchRows(ctx context.Context, fetcher Fetcher, ignoreLines int) ([][]interface{}, error) {
	rows, err := fetcher(ctx)
	if err != nil {
		return nil, err
	}
	return cfg.Columns.ToRows(rows, ignoreLines), nil
}

func (cfg *BaseTableConfig) ToTable() *psqlfront.Table {
	return &psqlfront.Table{
		SchemaName: cfg.schema,
		RelName:    cfg.Name,
		Columns:    cfg.Columns.ToColumns(),
	}
}

func (cfg *BaseTableConfig) DetectSchema(ctx context.Context, fetcher Fetcher, ignoreLines int, allowUnicodeColumnName bool) error {
	rows, err := fetcher(ctx)
	if err != nil {
		return err
	}
	columns, err := PerformSchemaInference(rows, ignoreLines, allowUnicodeColumnName)
	if err != nil {
		remoteAddr := psqlfront.GetRemoteAddr(ctx)
		log.Printf("[warn][%s] perform cchema inference: %v", remoteAddr, err)
		return nil
	}
	cfg.Columns = columns
	return nil
}
