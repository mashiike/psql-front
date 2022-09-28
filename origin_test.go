package psqlfront_test

import (
	"context"
	"testing"

	psqlfront "github.com/mashiike/psql-front"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type DummyOrigin struct {
	id     string
	tables []*psqlfront.Table
}

func (o *DummyOrigin) ID() string {
	return o.id
}

func (o *DummyOrigin) GetTables(ctx context.Context) ([]*psqlfront.Table, error) {
	return o.tables, nil
}

func (o *DummyOrigin) RefreshCache(ctx context.Context, w psqlfront.CacheWriter) error {
	table := w.TargetTable()
	if err := w.DeleteRows(ctx); err != nil {
		return err
	}
	row := make([]interface{}, 0, len(table.Columns))
	for _, c := range table.Columns {
		row = append(row, "value_"+c.Name)
	}
	w.AppendRows(ctx, [][]interface{}{row})
	return nil
}

type DummyOriginConfig struct {
	Schema string   `yaml:"schema"`
	Tables []string `yaml:"tables"`
}

const DummyOriginType = "Dummy"

func (cfg *DummyOriginConfig) Type() string {
	return DummyOriginType
}

func (cfg *DummyOriginConfig) Restrict() error {
	return nil
}

func (cfg *DummyOriginConfig) NewOrigin(id string) (psqlfront.Origin, error) {
	return &DummyOrigin{
		id: id,
		tables: lo.Map(cfg.Tables, func(table string, _ int) *psqlfront.Table {
			l := int(256)
			return &psqlfront.Table{
				SchemaName: cfg.Schema,
				RelName:    table,
				Columns: []*psqlfront.Column{
					{
						Name:     "id",
						DataType: "varchar",
						Length:   &l,
					},
				},
			}
		}),
	}, nil
}

func TestDummyOriginConfigUnmarshalSuccess(t *testing.T) {
	psqlfront.RegisterOriginType(DummyOriginType, func() psqlfront.OriginConfig {
		return &DummyOriginConfig{}
	})
	defer psqlfront.UnregisterOriginType(DummyOriginType)

	cfgString := []byte(`
id: hoge
type: Dummy
schema: psqlfront_test
tables:
  - dummy
  - hoge
`)
	var cfg psqlfront.CommonOriginConfig
	err := yaml.Unmarshal(cfgString, &cfg)
	require.NoError(t, err)
	require.EqualValues(t, DummyOriginType, cfg.Type)
	require.EqualValues(t, DummyOriginType, cfg.OriginConfig.Type())
	origin, err := cfg.NewOrigin()
	require.NoError(t, err)
	l := int(256)
	expected := &DummyOrigin{
		id: "hoge",
		tables: []*psqlfront.Table{
			{
				SchemaName: "psqlfront_test",
				RelName:    "dummy",
				Columns: []*psqlfront.Column{
					{
						Name:     "id",
						DataType: "varchar",
						Length:   &l,
					},
				},
			},
			{
				SchemaName: "psqlfront_test",
				RelName:    "hoge",
				Columns: []*psqlfront.Column{
					{
						Name:     "id",
						DataType: "varchar",
						Length:   &l,
					},
				},
			},
		},
	}
	require.EqualValues(t, expected, origin)
}
