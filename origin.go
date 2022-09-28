package psqlfront

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type CacheWriter interface {
	DeleteRows(ctx context.Context) error
	ReplaceCacheTable(ctx context.Context, t *Table) error
	AppendRows(context.Context, [][]interface{}) error
	TargetTable() *Table
}

type Origin interface {
	ID() string
	GetTables(ctx context.Context) ([]*Table, error)
	RefreshCache(context.Context, CacheWriter) error
}

type OriginConfig interface {
	Type() string
	Restrict() error
	NewOrigin(id string) (Origin, error)
}

var originConfigConstructors = make(map[string]func() OriginConfig)

func RegisterOriginType(typeName string, originConfigConstructor func() OriginConfig) {
	originConfigConstructors[typeName] = originConfigConstructor
}

func UnregisterOriginType(typeName string) {
	delete(originConfigConstructors, typeName)
}

func GetOriginConfig(typeName string) (OriginConfig, bool) {
	originConfigConstructor, ok := originConfigConstructors[typeName]
	if !ok {
		return nil, false
	}
	if originConfigConstructor == nil {
		return nil, true
	}
	return originConfigConstructor(), true
}

type CommonOriginConfig struct {
	ID   string         `yaml:"id"`
	Type string         `yaml:"type"`
	TTL  *time.Duration `yaml:"ttl"`

	OriginConfig OriginConfig `yaml:"-"`
}

func (cfg *CommonOriginConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type alias CommonOriginConfig
	var aux alias
	if err := unmarshal(&aux); err != nil {
		return fmt.Errorf("aux decode: %w", err)
	}
	*cfg = CommonOriginConfig(aux)
	originCfg, ok := GetOriginConfig(cfg.Type)
	if !ok {
		return fmt.Errorf("type `%s` not registerd", cfg.Type)
	}
	if originCfg == nil {
		return fmt.Errorf("type `%s` is invalid", cfg.Type)
	}
	cfg.OriginConfig = originCfg
	return unmarshal(originCfg)
}

func (cfg *CommonOriginConfig) Ristrict() error {
	if cfg.ID == "" {
		return errors.New("origin_id is required")
	}
	if cfg.OriginConfig == nil {
		return errors.New("origin config is nil")
	}
	if cfg.Type != cfg.OriginConfig.Type() {
		return errors.New("origin type missmatch")
	}
	return cfg.OriginConfig.Restrict()
}

func (cfg *CommonOriginConfig) NewOrigin() (Origin, error) {
	return cfg.OriginConfig.NewOrigin(cfg.ID)
}
