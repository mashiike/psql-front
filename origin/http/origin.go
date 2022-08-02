package static

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	psqlfront "github.com/mashiike/psql-front"
	encoding "github.com/mattn/go-encoding"
	"github.com/samber/lo"
	"golang.org/x/net/html/charset"
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
		return cfg.toTableInfo(o.schema)
	}), nil
}

func (o *Origin) GetRows(ctx context.Context, w psqlfront.CacheWriter, table *psqlfront.Table) error {
	if o.schema != table.SchemaName {
		return psqlfront.WrapOriginNotFoundError(errors.New("origin schema is missmatch"))
	}
	for _, t := range o.tables {
		if t.Name != table.RelName {
			continue
		}
		return o.getRows(ctx, w, t, table)
	}
	return psqlfront.WrapOriginNotFoundError(errors.New("origin table not found"))
}
func (o *Origin) getRows(ctx context.Context, w psqlfront.CacheWriter, cfg *TableConfig, table *psqlfront.Table) error {
	remoteAddr := psqlfront.GetRemoteAddr(ctx)
	log.Printf("[debug][%s] http request: GET %s", remoteAddr, cfg.URL)
	resp, err := http.Get(cfg.URL)
	if err != nil {
		return fmt.Errorf("try get %s origin: GET %s failed: %v", table.String(), cfg.URL, err)
	}
	if resp.StatusCode < http.StatusOK && resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("try get %s origin: GET %s failed: %d %s", table.String(), cfg.URL, resp.StatusCode, resp.Status)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	var r io.Reader = br
	if cfg.TextEncoding != nil {
		if enc := encoding.GetEncoding(*cfg.TextEncoding); enc != nil {
			r = enc.NewDecoder().Reader(br)
		}
	}
	if r == br {
		if data, err2 := br.Peek(1024); err2 == nil {
			enc, name, _ := charset.DetermineEncoding(data, resp.Header.Get("Content-Type"))
			if enc != nil {
				log.Printf("[debug][%s] content-encoding: %v", remoteAddr, enc)
				r = enc.NewDecoder().Reader(br)

			} else if len(name) > 0 {
				log.Printf("[debug][%s] content-encoding: name = %s", remoteAddr, name)
				if enc := encoding.GetEncoding(name); enc != nil {
					r = enc.NewDecoder().Reader(br)
				}
			} else {
				log.Printf("[debug][%s] content-encoding: not set", remoteAddr)
			}
		}
	}

	switch cfg.Format {
	default:
		reader := csv.NewReader(r)
		records, err := reader.ReadAll()
		if err != nil {
			return fmt.Errorf("try get %s origin: GET %s: parse failed: %w", table.String(), cfg.URL, err)
		}
		if cfg.IgnoreLines > 0 {
			if cfg.IgnoreLines >= len(records) {
				return nil
			}
			records = records[cfg.IgnoreLines:]
		}
		return w.AppendRows(ctx, lo.Map(records, func(record []string, _ int) []interface{} {
			log.Printf("[debug][%s] row: %v", remoteAddr, record)
			row := make([]interface{}, 0, len(cfg.Columns))
			for i, c := range cfg.Columns {
				if c.ColumnIndex != nil {
					if *c.ColumnIndex < len(record) {
						row = append(row, record[*c.ColumnIndex])
					} else {
						row = append(row, nil)
					}
					continue
				}
				if i < len(record) {
					row = append(row, record[i])
				} else {
					row = append(row, nil)
				}
			}
			return row
		}))
	}
}

type OriginConfig struct {
	Schema string         `yaml:"schema"`
	Tables []*TableConfig `yaml:"tables"`
}

type TableConfig struct {
	Name         string          `yaml:"name,omitempty"`
	Columns      []*ColumnConfig `yaml:"columns,omitempty"`
	URL          string          `yaml:"url"`
	Format       string          `yaml:"format"`
	IgnoreLines  int             `yaml:"ignore_lines"`
	TextEncoding *string         `yaml:"text_encoding"`
	urlObj       *url.URL        `yaml:"-"`
}

type ColumnConfig struct {
	Name        string `yaml:"name,omitempty"`
	DataType    string `yaml:"data_type,omitempty"`
	DataLength  *int   `yaml:"length,omitempty"`
	Contraint   string `yaml:"contraint,omitempty"`
	ColumnIndex *int   `yaml:"column_index,omitempty"`
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
		if table.Format == "" {
			table.Format = "csv"
		}
		if table.URL == "" {
			return fmt.Errorf("table[%d]: url is required", i)
		}
		var err error
		if table.urlObj, err = url.Parse(table.URL); err != nil {
			return fmt.Errorf("table[%d]: url is invalid: %v", i, err)
		}
		if table.urlObj.Scheme != "http" && table.urlObj.Scheme != "https" {
			return fmt.Errorf("table[%d]: url.schema must http/https", i)
		}
		if len(table.Columns) == 0 {
			return fmt.Errorf("table[%d].columns: empty", i)
		}
		for j, column := range table.Columns {
			if column.Name == "" {
				return fmt.Errorf("table[%d:%s].column[%d]: name is required", i, table.Name, j)
			}
			if column.DataType == "" {
				column.DataType = "TEXT"
			}
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

func (cfg *TableConfig) toTableInfo(schema string) *psqlfront.Table {
	return &psqlfront.Table{
		SchemaName: schema,
		RelName:    cfg.Name,
		Columns: lo.Map(cfg.Columns, func(column *ColumnConfig, _ int) *psqlfront.Column {
			return &psqlfront.Column{
				Name:      column.Name,
				DataType:  column.DataType,
				Length:    column.DataLength,
				Contraint: column.Contraint,
			}
		}),
	}
}
