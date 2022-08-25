package origin

import (
	"context"
	"fmt"
	"io"
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

type Fetcher func(context.Context) (io.ReadCloser, error)
type Parser func(context.Context, io.Reader) ([][]string, error)

func (cfg *BaseTableConfig) FetchRows(ctx context.Context, fetcher Fetcher, parser Parser, ignoreLines int) ([][]interface{}, error) {
	rows, err := cfg.fetchRows(ctx, fetcher, parser)
	if err != nil {
		return nil, err
	}
	return cfg.Columns.ToRows(rows, ignoreLines), nil
}

func (cfg *BaseTableConfig) fetchRows(ctx context.Context, fetcher Fetcher, parser Parser) ([][]string, error) {
	reader, err := fetcher(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	rows, err := parser(ctx, reader)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (cfg *BaseTableConfig) ToTable() *psqlfront.Table {
	return &psqlfront.Table{
		SchemaName: cfg.schema,
		RelName:    cfg.Name,
		Columns:    cfg.Columns.ToColumns(),
	}
}

func (cfg *BaseTableConfig) DetectSchema(ctx context.Context, fetcher Fetcher, parser Parser, ignoreLines int) error {
	rows, err := cfg.fetchRows(ctx, fetcher, parser)
	if err != nil {
		return err
	}
	columns, err := PerformSchemaInference(rows, ignoreLines)
	if err != nil {
		remoteAddr := psqlfront.GetRemoteAddr(ctx)
		log.Printf("[warn][%s] perform cchema inference: %v", remoteAddr, err)
		return nil
	}
	cfg.Columns = columns
	return nil
}
