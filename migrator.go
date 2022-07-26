package psqlfront

import (
	_ "embed"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/k0kubun/sqldef/database"
	"github.com/k0kubun/sqldef/database/postgres"
	"github.com/k0kubun/sqldef/parser"
	"github.com/k0kubun/sqldef/schema"
	"github.com/samber/lo"
)

type Migrator struct {
	parser     database.GenericParser
	target     database.Config
	disableSSL bool
}

func NewMigrator(cfg *CacheDatabaseConfig) *Migrator {
	sqlParser := database.NewParser(parser.ParserModePostgres)
	return &Migrator{
		parser: sqlParser,
		target: database.Config{
			DbName:   cfg.Database,
			User:     cfg.Username,
			Password: cfg.Password,
			Host:     cfg.Host,
			Port:     cfg.Port,
		},
		disableSSL: cfg.SSLMode == "disable",
	}
}

//go:embed sql/psqlfront.sql
var baseDDL string

func (m *Migrator) GenerateDesiredDDLs(tables []*Table) (string, error) {
	var builder strings.Builder
	builder.WriteString(baseDDL)
	builder.WriteRune('\n')
	for _, table := range tables {
		ddl, err := table.GenerateDDL()
		if err != nil {
			return "", fmt.Errorf("table `%s`:%w", table.String(), err)
		}
		builder.WriteString(ddl)
		builder.WriteRune('\n')
	}
	return builder.String(), nil
}

func (m *Migrator) GenerateCreateSchemaStmt(tables []*Table) string {
	if tables == nil {
		tables = make([]*Table, 0)
	}
	schemas := lo.Uniq(append([]string{"psqlfront"}, lo.Uniq(lo.Map(tables, func(t *Table, _ int) string {
		return t.SchemaName
	}))...))
	var builder strings.Builder
	for _, schema := range schemas {
		fmt.Fprintf(&builder, `CREATE SCHEMA IF NOT EXISTS "%s";`, schema)
		builder.WriteRune('\n')
	}
	return builder.String()
}

func (m *Migrator) ExecuteMigration(tables []*Table) error {
	desiredDDLs, err := m.GenerateDesiredDDLs(tables)
	if err != nil {
		return err
	}
	beforeApply := m.GenerateCreateSchemaStmt(tables)
	return m.executeMigration(desiredDDLs, beforeApply)
}

func (m *Migrator) executeMigration(desiredDDLs string, beforeApply string) error {
	if m.disableSSL {
		originalEnv := os.Getenv("PGSSLMODE")
		os.Setenv("PGSSLMODE", "disable")
		defer func() {
			os.Setenv("PGSSLMODE", originalEnv)
		}()
	}
	targetDB, err := postgres.NewDatabase(m.target)
	if err != nil {
		return err
	}
	defer targetDB.Close()
	currentDDLs, err := targetDB.DumpDDLs()
	if err != nil {
		return fmt.Errorf("failed dump target db ddls:%w", err)
	}

	ddls, err := schema.GenerateIdempotentDDLs(schema.GeneratorModePostgres, m.parser, desiredDDLs, currentDDLs, database.GeneratorConfig{})
	if err != nil {
		return fmt.Errorf("generate idempotent DDLs:%w", err)
	}
	if len(ddls) == 0 {
		log.Println("[info] migration nothing to do")
		return nil
	}
	log.Printf("[info] need execute migration")

	if err := database.RunDDLs(targetDB, ddls, false, beforeApply); err != nil {
		return fmt.Errorf("run DDLs:%w", err)
	}
	return nil
}
