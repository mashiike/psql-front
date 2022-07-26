package psqlfront

import (
	"fmt"
	"log"
	"strings"
	"time"

	gv "github.com/hashicorp/go-version"
	gc "github.com/kayac/go-config"
)

type Config struct {
	RequiredVersion string `yaml:"required_version,omitempty"`

	CacheDatabase *CacheDatabaseConfig  `yaml:"cache_database,omitempty"`
	Certificates  []*CertificateConfig  `yaml:"certificates,omitempty"`
	DefaultTTL    time.Duration         `yaml:"default_ttl,omitempty"`
	Origins       []*CommonOriginConfig `yaml:"origins,omitempty"`

	versionConstraints gv.Constraints `yaml:"version_constraints,omitempty"`
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
	}
}

// Load loads configuration file from file paths.
func (c *Config) Load(path string) error {
	if err := gc.LoadWithEnv(c, path); err != nil {
		return err
	}
	return c.Restrict()
}

func (cfg *Config) Restrict() error {
	if cfg.RequiredVersion != "" {
		constraints, err := gv.NewConstraint(cfg.RequiredVersion)
		if err != nil {
			return fmt.Errorf("required_version has invalid format: %w", err)
		}
		cfg.versionConstraints = constraints
	}

	for i, originCfg := range cfg.Origins {
		if originCfg.TTL == nil {
			originCfg.TTL = &cfg.DefaultTTL
		}
		if err := originCfg.Ristrict(); err != nil {
			return fmt.Errorf("origins[%d]:%w", i, err)
		}
	}
	return nil
}

// ValidateVersion validates a version satisfies required_version.
func (cfg *Config) ValidateVersion(version string) error {
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
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)
}

type CertificateConfig struct {
	Cert string `yaml:"cert,omitempty"`
	Key  string `yaml:"key,omitempty"`
}
