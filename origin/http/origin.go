package http

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Songmu/flextime"
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
		return cfg.ToTable()
	}), nil
}

func (o *Origin) MigrateTable(ctx context.Context, m psqlfront.CacheMigrator, table *psqlfront.Table) error {
	if o.schema != table.SchemaName {
		return psqlfront.WrapOriginNotFoundError(errors.New("origin schema is missmatch"))
	}
	for _, t := range o.tables {
		if t.Name != table.RelName {
			continue
		}
		if !t.SchemaDetection {
			return nil
		}
		if err := t.DetectSchema(ctx); err != nil {
			return err
		}
		return m.Migrate(ctx, t.ToTable())
	}
	return psqlfront.WrapOriginNotFoundError(errors.New("origin table not found"))
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
	rows, err := cfg.FetchRows(ctx)
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
	origin.BaseTableConfig `yaml:",inline"`

	URLString                string        `yaml:"url"`
	Format                   string        `yaml:"format"`
	IgnoreLines              int           `yaml:"ignore_lines"`
	TextEncoding             *string       `yaml:"text_encoding"`
	SchemaDetection          bool          `yaml:"schema_detection"`
	DetectedSchemaExpiration time.Duration `yaml:"detected_schema_expiration"`
	URL                      *url.URL      `yaml:"-"`
	LastSchemaDetection      time.Time     `yaml:"-"`
}

func (cfg *OriginConfig) Type() string {
	return OriginType
}

func (cfg *OriginConfig) Restrict() error {
	if cfg.Schema == "" {
		cfg.Schema = "public"
	}
	for i, table := range cfg.Tables {
		if err := table.Restrict(cfg.Schema); err != nil {
			return fmt.Errorf("table[%d]: %w", i, err)
		}
	}
	return nil
}

var allowedSchemas = []string{"http", "https"}

func (cfg *TableConfig) Restrict(schema string) error {
	if cfg.URLString == "" {
		return fmt.Errorf("url is required")
	}
	if cfg.Format == "" {
		cfg.Format = "csv"
	}
	var err error
	if cfg.URL, err = url.Parse(cfg.URLString); err != nil {
		return fmt.Errorf("url is invalid: %v", err)
	}
	if !lo.Contains(allowedSchemas, cfg.URL.Scheme) {
		return fmt.Errorf("url.schema must %s", strings.Join(allowedSchemas, "/"))
	}
	if !cfg.SchemaDetection {
		if len(cfg.Columns) == 0 {
			return fmt.Errorf("columns: empty")
		}
	} else {
		if cfg.DetectedSchemaExpiration == 0 {
			cfg.DetectedSchemaExpiration = 24 * time.Hour
		}
		if err := cfg.DetectSchema(context.Background()); err != nil {
			return fmt.Errorf("initial schema detect: %w", err)
		}
	}
	if err := cfg.BaseTableConfig.Restrict(schema); err != nil {
		return err
	}
	return nil
}

func (cfg *TableConfig) Fetcher(ctx context.Context) ([][]string, error) {
	remoteAddr := psqlfront.GetRemoteAddr(ctx)
	log.Printf("[debug][%s] http request: GET %s", remoteAddr, cfg.URL)
	resp, err := http.Get(cfg.URL.String())
	if err != nil {
		return nil, fmt.Errorf("GET %s failed: %v", cfg.URL, err)
	}
	if resp.StatusCode < http.StatusOK && resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("GET %s failed: %d %s", cfg.URL, resp.StatusCode, resp.Status)
	}
	defer resp.Body.Close()
	tr := origin.ConvertTextEncoding(resp.Body, cfg.TextEncoding)
	switch cfg.Format {
	case "csv", "CSV":
		reader := csv.NewReader(tr)
		return reader.ReadAll()
	}
	return nil, errors.New("unexpected format")
}

func (cfg *TableConfig) FetchRows(ctx context.Context) ([][]interface{}, error) {
	return cfg.BaseTableConfig.FetchRows(ctx, cfg.Fetcher, cfg.IgnoreLines)
}

func (cfg *TableConfig) DetectSchema(ctx context.Context) error {
	now := flextime.Now()
	if now.Sub(cfg.LastSchemaDetection) < cfg.DetectedSchemaExpiration {
		return nil
	}
	if err := cfg.BaseTableConfig.DetectSchema(ctx, cfg.Fetcher, cfg.IgnoreLines); err != nil {
		return err
	}
	cfg.LastSchemaDetection = now
	return nil
}

func (cfg *OriginConfig) NewOrigin(id string) (psqlfront.Origin, error) {
	return &Origin{
		id:     id,
		schema: cfg.Schema,
		tables: cfg.Tables,
	}, nil
}
