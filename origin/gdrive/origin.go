package gdrive

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Songmu/flextime"
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
	id     string
	schema string
	tables []*TableConfig
}

func (o *Origin) GetTables(_ context.Context) ([]*psqlfront.Table, error) {
	return lo.Map(o.tables, func(cfg *TableConfig, _ int) *psqlfront.Table {
		return cfg.ToTable()
	}), nil
}

func (o *Origin) RefreshCache(ctx context.Context, w psqlfront.CacheWriter) error {
	table := w.TargetTable()
	if o.schema != table.SchemaName {
		return psqlfront.WrapOriginNotFoundError(errors.New("origin schema is missmatch"))
	}
	for _, t := range o.tables {
		if t.Name != table.RelName {
			continue
		}
		return o.refreshCache(ctx, w, t)
	}
	return psqlfront.WrapOriginNotFoundError(errors.New("origin table not found"))
}

func (o *Origin) refreshCache(ctx context.Context, w psqlfront.CacheWriter, cfg *TableConfig) error {
	remoteAddr := psqlfront.GetRemoteAddr(ctx)
	if cfg.SchemaDetection {
		if err := cfg.DetectSchema(ctx); err != nil {
			return err
		}
		if err := w.ReplaceCacheTable(ctx, cfg.ToTable()); err != nil {
			return err
		}
	} else {
		if err := w.DeleteRows(ctx); err != nil {
			return err
		}
	}

	log.Printf("[debug][%s] get rows: file_id=%s", remoteAddr, cfg.FileID)
	rows, err := cfg.FetchRows(ctx)
	if err != nil {
		return fmt.Errorf("try get %s origin: %w", w.TargetTable().String(), err)
	}
	return w.AppendRows(ctx, rows)
}

type OriginConfig struct {
	Schema string         `yaml:"schema"`
	Tables []*TableConfig `yaml:"tables"`
}

type TableConfig struct {
	origin.BaseTableConfig `yaml:",inline"`
	FileType               string `yaml:"file_type"`
	FileID                 string `yaml:"-"`
	Range                  string `yaml:"range,omitempty"`

	URLString                string        `yaml:"url"`
	IgnoreLines              int           `yaml:"ignore_lines"`
	SchemaDetection          bool          `yaml:"schema_detection"`
	DetectedSchemaExpiration time.Duration `yaml:"detected_schema_expiration"`
	URL                      *url.URL      `yaml:"-"`
	LastSchemaDetection      time.Time     `yaml:"-"`

	driveSvc  *drive.Service  `yaml:"-"`
	sheetsSvc *sheets.Service `yaml:"-"`
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
	ctx := context.Background()
	gcpOpts := []option.ClientOption{
		option.WithScopes(
			drive.DriveReadonlyScope,
		),
	}
	if credentials := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON"); credentials != "" {
		gcpOpts = append(gcpOpts, option.WithCredentialsJSON([]byte(credentials)))
	}
	driveSvc, err := drive.NewService(ctx, gcpOpts...)
	if err != nil {
		return fmt.Errorf("create Google Drive Service: %w", err)
	}
	sheetsSvc, err := sheets.NewService(ctx, gcpOpts...)
	if err != nil {
		return fmt.Errorf("create Google Sheets Service: %w", err)
	}
	for i, table := range cfg.Tables {
		if err := table.Restrict(cfg.Schema, driveSvc, sheetsSvc); err != nil {
			return fmt.Errorf("table[%d]: %w", i, err)
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

func (cfg *TableConfig) Restrict(schema string, driveSvc *drive.Service, sheetsSvc *sheets.Service) error {
	var err error
	cfg.driveSvc = driveSvc
	cfg.sheetsSvc = sheetsSvc
	if cfg.URL, err = url.Parse(cfg.URLString); err != nil {
		return fmt.Errorf("url is invalid: %v", err)
	}
	switch cfg.URL.Host {
	case "drive.google.com":
		if cfg.URL.Path != "/open" {
			return errors.New("invalid url, path is not /open")
		}
		cfg.FileID = cfg.URL.Query().Get("id")
		if cfg.FileID == "" {
			return errors.New("invalid url, FileID not found")
		}
	case "docs.google.com":
		if !strings.HasPrefix(cfg.URL.Path, "/spreadsheets/d/") {
			return errors.New("invalid url, FileID not found")
		}
		cfg.FileID = strings.Split(strings.TrimPrefix(cfg.URL.Path, "/spreadsheets/d/"), "/")[0]
		cfg.FileType = FileTypeSpreadsheets
	default:
		return errors.New("invalid url, not drive.google.com or docs.google.com")
	}
	if cfg.FileType == "" {
		cfg.FileType = FileTypeSpreadsheets
	}
	cfg.FileType = strings.ToLower(cfg.FileType)
	if !cfg.SchemaDetection {
		if len(cfg.Columns) == 0 {
			return fmt.Errorf("columns: empty")
		}
	} else {
		if cfg.DetectedSchemaExpiration == 0 {
			cfg.DetectedSchemaExpiration = 24 * time.Hour
		}
		if err := cfg.DetectSchema(context.Background()); err != nil {
			log.Printf("[warn] %s.%s initial schema detection failed: %v", schema, cfg.Name, err)
			cfg.Columns = origin.ColumnConfigs{
				{
					Name:     "dummy",
					DataType: "VARCHAR",
				},
			}
		}
	}
	if err := cfg.BaseTableConfig.Restrict(schema); err != nil {
		return err
	}
	return nil
}

func (cfg *TableConfig) Fetcher(ctx context.Context) ([][]string, error) {
	switch cfg.FileType {
	case FileTypeSpreadsheets:
		r := cfg.Range
		if r == "" {
			r = "A:ZZ"
		}
		resp, err := cfg.sheetsSvc.Spreadsheets.Values.Get(cfg.FileID, r).Context(ctx).Do()
		if err != nil {
			return nil, fmt.Errorf("can not get %s: %w", cfg.URLString, err)
		}
		rows := lo.Map(resp.Values, func(values []interface{}, _ int) []string {
			return lo.Map(values, func(value interface{}, _ int) string {
				s, ok := value.(string)
				if ok {
					return s
				}
				return fmt.Sprintf("%s", value)
			})
		})
		return rows, nil
	case FileTypeCSV:
		resp, err := cfg.driveSvc.Files.Export(cfg.FileID, "text/csv").Context(ctx).Download()
		if err != nil {
			return nil, fmt.Errorf("can not get %s: %w", cfg.URLString, err)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("can not get %s: http status: %s", cfg.URLString, resp.Status)
		}
		defer resp.Body.Close()
		tr := origin.ConvertTextEncoding(resp.Body, nil)
		reader := csv.NewReader(tr)
		return reader.ReadAll()
	default:
		return nil, errors.New("unexpected file type")
	}
}

func (cfg *TableConfig) FetchRows(ctx context.Context) ([][]interface{}, error) {
	return cfg.BaseTableConfig.FetchRows(ctx, cfg.Fetcher, cfg.IgnoreLines)
}

func (cfg *TableConfig) DetectSchema(ctx context.Context) error {
	remoteAddr := psqlfront.GetRemoteAddr(ctx)
	log.Printf("[debug][%s] try detect schema: file_id=%s", remoteAddr, cfg.FileID)
	now := flextime.Now()
	if now.Sub(cfg.LastSchemaDetection) < cfg.DetectedSchemaExpiration {
		log.Printf("[debug][%s] skip detect schema: file_id=%s", remoteAddr, cfg.FileID)
		return nil
	}
	log.Printf("[debug][%s] start detect schema: file_id=%s", remoteAddr, cfg.FileID)
	if err := cfg.BaseTableConfig.DetectSchema(ctx, cfg.Fetcher, cfg.IgnoreLines); err != nil {
		return err
	}
	log.Printf("[debug][%s] end detect schema: file_id=%s", remoteAddr, cfg.FileID)
	cfg.LastSchemaDetection = now
	return nil
}
