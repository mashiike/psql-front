package static

import (
	"context"
	"errors"
	"fmt"
	"log"

	psqlfront "github.com/mashiike/psql-front"
	"github.com/mashiike/psql-front/origin"
	"github.com/samber/lo"
)

const OriginType = "Static"

func init() {
	psqlfront.RegisterOriginType(OriginType, func() psqlfront.OriginConfig {
		return &OriginConfig{}
	})
	log.Printf("[info] load origin type: %s", OriginType)
}

type Origin struct {
	id     string
	tables []*psqlfront.Table
	rows   map[string][][]string
}

func (o *Origin) ID() string {
	return o.id
}

func (o *Origin) GetTables(_ context.Context) ([]*psqlfront.Table, error) {
	return o.tables, nil
}

func (o *Origin) RefreshCache(ctx context.Context, w psqlfront.CacheWriter) error {
	table := w.TargetTable()
	if err := w.DeleteRows(ctx); err != nil {
		return err
	}
	rows, ok := o.rows[table.String()]
	if !ok {
		psqlfront.WrapOriginNotFoundError(errors.New("table not found"))
	}
	return w.AppendRows(ctx, lo.Map(rows, func(row []string, _ int) []interface{} {
		log.Printf("[debug] row: %v", row)
		return lo.Map(row, func(v string, _ int) interface{} {
			return v
		})
	}))
}

type OriginConfig struct {
	Schema string         `yaml:"schema"`
	Tables []*TableConfig `yaml:"tables"`
}

type TableConfig struct {
	Name    string               `yaml:"name,omitempty"`
	Columns origin.ColumnConfigs `yaml:"columns,omitempty"`
	Rows    [][]string           `yaml:"rows,omitempty"`
}

func (cfg *OriginConfig) Type() string {
	return OriginType
}

func (cfg *OriginConfig) Restrict() error {
	if cfg.Schema == "" {
		cfg.Schema = "public"
	}
	for i, table := range cfg.Tables {
		if table.Name == "" {
			return fmt.Errorf("table[%d]: name is required", i)
		}
		if len(table.Columns) == 0 {
			return fmt.Errorf("table[%d].columns: empty", i)
		}
		if err := table.Columns.Restrict(); err != nil {
			return fmt.Errorf("table[%d:%s].%w", i, table.Name, err)
		}
	}
	return nil
}

func (cfg *OriginConfig) NewOrigin(id string) (psqlfront.Origin, error) {
	return &Origin{
		id: id,
		tables: lo.Map(cfg.Tables, func(table *TableConfig, _ int) *psqlfront.Table {
			return &psqlfront.Table{
				SchemaName: cfg.Schema,
				RelName:    table.Name,
				Columns:    table.Columns.ToColumns(),
			}
		}),
		rows: lo.FromEntries(lo.Map(cfg.Tables, func(table *TableConfig, _ int) lo.Entry[string, [][]string] {
			return lo.Entry[string, [][]string]{
				Key: (&psqlfront.Table{
					SchemaName: cfg.Schema,
					RelName:    table.Name,
				}).String(),
				Value: table.Rows,
			}
		})),
	}, nil
}
