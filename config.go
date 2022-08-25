package psqlfront

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	gv "github.com/hashicorp/go-version"
	gc "github.com/kayac/go-config"
)

type Config struct {
	RequiredVersion string `yaml:"required_version,omitempty"`

	CacheDatabase *CacheDatabaseConfig  `yaml:"cache_database,omitempty"`
	Certificates  []*CertificateConfig  `yaml:"certificates,omitempty"`
	DefaultTTL    time.Duration         `yaml:"default_ttl,omitempty"`
	Origins       []*CommonOriginConfig `yaml:"origins,omitempty"`

	IdleTimeout *time.Duration `yaml:"idle_timeout,omitempty"`
	Stats       *StatsConfig   `yaml:"stats,omitempty"`

	versionConstraints gv.Constraints `yaml:"-,omitempty"`
}

func PtrValue[T any](t T) *T {
	return &t
}

func DefaultConfig() *Config {
	return &Config{
		CacheDatabase: &CacheDatabaseConfig{
			Host:     "localhost",
			Username: "postgres",
			Password: "postgres",
			Port:     5432,
			Database: "postgres",
			SSLMode:  "prefer",
		},
		DefaultTTL: 24 * time.Hour,
		Stats: &StatsConfig{
			Enabled:            PtrValue(true),
			MonitoringInterval: 30 * time.Second,
		},
		IdleTimeout: PtrValue(600 * time.Second),
	}
}

// Load loads configuration file from file paths.
func (cfg *Config) Load(path string) error {
	src, err := loadSrcFrom(path)
	if err != nil {
		return err
	}
	err = gc.LoadWithEnvBytes(cfg, src)
	if err != nil {
		return err
	}
	if err := cfg.Restrict(); err != nil {
		return err
	}
	return nil
}

func (cfg *Config) Restrict() error {
	if cfg.RequiredVersion != "" {
		constraints, err := gv.NewConstraint(cfg.RequiredVersion)
		if err != nil {
			return fmt.Errorf("required_version has invalid format: %w", err)
		}
		cfg.versionConstraints = constraints
	}

	for i, certCfg := range cfg.Certificates {
		certPEMBlock, err := loadSrcFrom(certCfg.Cert)
		if err != nil {
			return fmt.Errorf("certificates[%d]: cert can not load:%w", i, err)
		}
		keyPEMBlock, err := loadSrcFrom(certCfg.Key)
		if err != nil {
			return fmt.Errorf("certificates[%d]: key can not load:%w", i, err)
		}
		certificate, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
		if err != nil {
			return err
		}
		cfg.Certificates[i].certificate = certificate
	}

	for i, originCfg := range cfg.Origins {
		if originCfg.TTL == nil {
			originCfg.TTL = &cfg.DefaultTTL
		}
		if err := originCfg.Ristrict(); err != nil {
			return fmt.Errorf("origins[%d]:%w", i, err)
		}
	}
	if cfg.Stats == nil {
		cfg.Stats = &StatsConfig{
			Enabled: PtrValue(false),
		}
	}
	if cfg.Stats.MonitoringInterval == 0 {
		cfg.Stats.Enabled = PtrValue(false)
	}
	if cfg.Stats.Enabled == nil {
		cfg.Stats.Enabled = PtrValue(cfg.Stats.MonitoringInterval >= 1*time.Second)
	}
	if cfg.Stats.MonitoringInterval >= 24*time.Hour {
		log.Println("[warn] stats.monitoring_interval can not set over 24 hours")
		cfg.Stats.MonitoringInterval = 24 * time.Hour

	}
	return cfg.validateVersion(Version)
}

// ValidateVersion validates a version satisfies required_version.
func (cfg *Config) validateVersion(version string) error {
	if cfg.versionConstraints == nil {
		log.Println("[warn] required_version is empty. Skip checking required_version.")
		return nil
	}
	versionParts := strings.SplitN(version, "-", 2)
	v, err := gv.NewVersion(versionParts[0])
	if err != nil {
		log.Printf("[warn]: Invalid version format \"%s\". Skip checking required_version.", version)
		// invalid version string (e.g. "current") always allowed
		return nil
	}
	if !cfg.versionConstraints.Check(v) {
		return fmt.Errorf("version %s does not satisfy constraints required_version: %s", version, cfg.versionConstraints)
	}
	return nil
}

type CacheDatabaseConfig struct {
	Host     string `yaml:"host,omitempty"`
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
	Port     int    `yaml:"port,omitempty"`
	Database string `yaml:"database,omitempty"`
	SSLMode  string `yaml:"ssl_mode,omitempty"`
}

func (cfg *CacheDatabaseConfig) DSN() string {
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)
	if cfg.SSLMode != "" {
		dsn += fmt.Sprintf("?sslmode=%s", cfg.SSLMode)
	}
	return dsn
}

type StatsConfig struct {
	Enabled            *bool         `yaml:"enabled,omitempty"`
	MonitoringInterval time.Duration `yaml:"monitoring_interval,omitempty"`
	StoreDatabase      bool          `yaml:"store_database,omitempty"`
}

func (cfg *StatsConfig) enabled() bool {
	if cfg == nil {
		return false
	}
	if cfg.Enabled == nil {
		return false
	}
	return *cfg.Enabled
}

type CertificateConfig struct {
	Cert string `yaml:"cert,omitempty"`
	Key  string `yaml:"key,omitempty"`

	certificate tls.Certificate
}

func loadSrcFrom(path string) ([]byte, error) {
	u, err := url.Parse(path)
	if err != nil {
		// not a URL. load as a file path
		return os.ReadFile(path)
	}
	switch u.Scheme {
	case "http", "https":
		return fetchHTTP(u)
	case "s3":
		return fetchS3(u)
	case "gcs":
		return fetchGCS(u)
	case "file", "":
		return os.ReadFile(u.Path)
	default:
		return nil, fmt.Errorf("scheme %s is not supported", u.Scheme)
	}
}

func fetchHTTP(u *url.URL) ([]byte, error) {
	log.Println("[info] fetching from", u)
	resp, err := http.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func fetchS3(u *url.URL) ([]byte, error) {
	log.Println("[info] fetching from", u)
	ctx := context.Background()
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load default aws config, %w", err)
	}
	client := s3.NewFromConfig(awsCfg)
	bucket := u.Host
	key := strings.TrimLeft(u.Path, "/")
	headObject, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to head object from S3, %w", err)
	}
	buf := make([]byte, int(headObject.ContentLength))
	w := manager.NewWriteAtBuffer(buf)
	downloader := manager.NewDownloader(client)
	_, err = downloader.Download(ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from S3, %s", err)
	}
	return buf, nil
}

func fetchGCS(u *url.URL) ([]byte, error) {
	log.Println("[info] fetching from", u)
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load default gcp config, %w", err)
	}

	obj := client.Bucket(u.Host).Object(strings.TrimLeft(u.Path, "/"))
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader, %w", err)
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
