package gdrive

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	psqlfront "github.com/mashiike/psql-front"
	"github.com/mashiike/psql-front/origin"
	"github.com/samber/lo"
	"google.golang.org/api/drive/v2"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

const (
	OriginType           = "GoogleDrive"
	FileTypeSpreadsheets = "spreadsheets"
	FileTypeCSV          = "csv"
)

func init() {
	psqlfront.RegisterOriginType(OriginType, func() psqlfront.OriginConfig {
		return &OriginConfig{}
	})
	log.Printf("[info] load origin type: %s", OriginType)
}

func (o *Origin) ID() string {
	return o.id
}

type Origin struct {
	driveSvc  *drive.Service
	sheetsSvc *sheets.Service
	id        string
	schema    string
	tables    []*TableConfig
}

func (o *Origin) GetTables(_ context.Context) ([]*psqlfront.Table, error) {
	return lo.Map(o.tables, func(cfg *TableConfig, _ int) *psqlfront.Table {
		return cfg.Base.ToTable(o.schema)
	}), nil
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
	var mimeType string
	switch cfg.FileType {
	case FileTypeSpreadsheets:
		r := cfg.Range
		if r == "" {
			r = "A:ZZ"
		}
		resp, err := o.sheetsSvc.Spreadsheets.Values.Get(cfg.FileID, r).Context(ctx).Do()
		if err != nil {
			return fmt.Errorf("can not get %s: %w", cfg.Base.URLString, err)
		}
		rows := cfg.Base.Columns.ToRowsWithoutConvert(resp.Values, cfg.Base.IgnoreLines)
		return w.AppendRows(ctx, rows)
	case FileTypeCSV:
		mimeType = "text/csv"
	default:
		return errors.New("unexpected file type")
	}
	rows, err := cfg.Base.FetchRows(ctx,
		func(ctx context.Context, u *url.URL) (io.ReadCloser, error) {
			resp, err := o.driveSvc.Files.Export(cfg.FileID, mimeType).Context(ctx).Download()
			if err != nil {
				return nil, fmt.Errorf("can not get %s: %w", cfg.Base.URLString, err)
			}
			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("can not get %s: http status: %s", cfg.Base.URLString, resp.Status)
			}
			return resp.Body, nil
		},
		func(_ context.Context, r io.Reader) ([][]string, error) {
			reader := csv.NewReader(r)
			return reader.ReadAll()
		},
	)
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
	Base     origin.RemoteTableConfig `yaml:",inline"`
	FileType string                   `yaml:"file_type"`
	FileID   string                   `yaml:"-"`
	Range    string                   `yaml:"range,omitempty"`
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
		if err := table.Restrict(); err != nil {
			return fmt.Errorf("table[%d]: %w", i, err)
		}
	}
	return nil
}

func (cfg *OriginConfig) NewOrigin(id string) (psqlfront.Origin, error) {
	ctx := context.Background()
	gcpOpts := []option.ClientOption{
		option.WithScopes(
			drive.DriveReadonlyScope,
		),
	}
	driveSvc, err := drive.NewService(ctx, gcpOpts...)
	if err != nil {
		return nil, fmt.Errorf("Create Google Drive Service: %w", err)
	}
	sheetsSvc, err := sheets.NewService(ctx, gcpOpts...)
	if err != nil {
		return nil, fmt.Errorf("Create Google Sheets Service: %w", err)
	}
	return &Origin{
		driveSvc:  driveSvc,
		sheetsSvc: sheetsSvc,
		id:        id,
		schema:    cfg.Schema,
		tables:    cfg.Tables,
	}, nil
}

func (cfg *TableConfig) Restrict() error {
	if err := cfg.Base.Restrict([]string{"http", "https"}); err != nil {
		return err
	}
	switch cfg.Base.URL.Host {
	case "drive.google.com":
		if cfg.Base.URL.Path != "/open" {
			return errors.New("invalid url, path is not /open")
		}
		cfg.FileID = cfg.Base.URL.Query().Get("id")
		if cfg.FileID == "" {
			return errors.New("invalid url, FileID not found")
		}
	case "docs.google.com":
		if !strings.HasPrefix(cfg.Base.URL.Path, "/spreadsheets/d/") {
			return errors.New("invalid url, FileID not found")
		}
		cfg.FileID = strings.Split(strings.TrimPrefix(cfg.Base.URL.Path, "/spreadsheets/d/"), "/")[0]
		cfg.FileType = FileTypeSpreadsheets
	default:
		return errors.New("invalid url, not drive.google.com or docs.google.com")
	}
	if cfg.FileType == "" {
		cfg.FileType = FileTypeSpreadsheets
	}
	cfg.FileType = strings.ToLower(cfg.FileType)
	return nil
}
