package psqlfront_test

import (
	"testing"

	psqlfront "github.com/mashiike/psql-front"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestConfigLoadNoError(t *testing.T) {
	psqlfront.RegisterOriginType(DummyOriginType, func() psqlfront.OriginConfig {
		return &DummyOriginConfig{}
	})
	defer psqlfront.UnregisterOriginType(DummyOriginType)
	cases := []struct {
		casename string
		path     string
		check    func(t *testing.T, cfg *psqlfront.Config)
	}{
		{
			casename: "empty config",
			path:     "testdata/config/empty.yaml",
		},
		{
			casename: "default config",
			path:     "testdata/config/default.yaml",
			check: func(t *testing.T, cfg *psqlfront.Config) {
				require.EqualValues(t, "postgres://postgres:postgres@localhost:5432/postgres?sslmode=prefer", cfg.CacheDatabase.DSN())
				require.EqualValues(t, []string{"dummy-example", "dummy-internal"}, lo.Map(cfg.Origins, func(o *psqlfront.CommonOriginConfig, _ int) string {
					return o.ID
				}))
				require.True(t, *cfg.Stats.Enabled)
			},
		},
		{
			casename: "if monitoring interval is zero,fallback enabled = false",
			path:     "testdata/config/monitoring_interval_zero.yaml",
			check: func(t *testing.T, cfg *psqlfront.Config) {
				require.False(t, *cfg.Stats.Enabled)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.casename, func(t *testing.T) {
			cfg := psqlfront.DefaultConfig()
			err := cfg.Load(c.path)
			require.NoError(t, err)
			if c.check != nil {
				c.check(t, cfg)
			}
		})
	}
}
