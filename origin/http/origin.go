package static

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"

	psqlfront "github.com/mashiike/psql-front"
	"github.com/mashiike/psql-front/origin"
	"github.com/samber/lo"
)

const OriginType = "HTTP"

func init() {
	psqlfront.RegisterOriginType(OriginType, func() psqlfront.OriginConfig {
		return &OriginConfig{}
	})
	log.Printf("[info] load origin type: %s", OriginType)
}

type Origin struct {
	id     string
	schema string
	tables []*TableConfig
}

func (o *Origin) ID() string {
	return o.id
}

func (o *Origin) GetTables(_ context.Context) ([]*psqlfront.Table, error) {
	return lo.Map(o.tables, func(cfg *TableConfig, _ int) *psqlfront.Table {
		return cfg.Base.ToTable(o.schema)
	}), nil
}

func (o *Origin) MigrateTable(ctx context.Context, _ psqlfront.CacheMigrator, _ *psqlfront.Table) error {
	return nil
}

func (o *Origin) GetRows(ctx context.Context, w psqlfront.CacheWriter, table *psqlfront.Table) error {
	if o.schema != table.SchemaName {
		return psqlfront.WrapOriginNotFoundError(errors.New("origin schema is missmatch"))
	}
	for _, t := range o.tables {
		if t.Base.Name != table.RelName {
			continue
		}
		return o.getRows(ctx, w, t, table)
	}
	return psqlfront.WrapOriginNotFoundError(errors.New("origin table not found"))
}

func (o *Origin) getRows(ctx context.Context, w psqlfront.CacheWriter, cfg *TableConfig, table *psqlfront.Table) error {
	rows, err := cfg.Base.FetchRows(ctx, nil, func(_ context.Context, r io.Reader) ([][]string, error) {
		switch cfg.Format {
		case "csv", "CSV":
			reader := csv.NewReader(r)
			return reader.ReadAll()
		}
		return nil, errors.New("unexpected format")
	})
	if err != nil {
		return fmt.Errorf("try get %s origin: %w", table.String(), err)
	}
	return w.AppendRows(ctx, rows)
}

type OriginConfig struct {
	Schema string         `yaml:"schema"`
	Tables []*TableConfig `yaml:"tables"`
}

type TableConfig struct {
	Base   origin.RemoteTableConfig `yaml:",inline"`
	Format string                   `yaml:"format"`
}

func (cfg *OriginConfig) Type() string {
	return OriginType
}

func (cfg *OriginConfig) Restrict() error {
	if cfg.Schema == "" {
		cfg.Schema = "public"
	}
	for i, table := range cfg.Tables {
		if err := table.Base.Restrict([]string{"http", "https"}); err != nil {
			return fmt.Errorf("table[%d]: %w", i, err)
		}
		if table.Format == "" {
			table.Format = "csv"
		}
	}
	return nil
}

func (cfg *OriginConfig) NewOrigin(id string) (psqlfront.Origin, error) {
	return &Origin{
		id:     id,
		schema: cfg.Schema,
		tables: cfg.Tables,
	}, nil
}
