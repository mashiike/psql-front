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
	preferSSL  bool
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
		preferSSL:  cfg.SSLMode == "prefer",
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
	return m.executeMigration(desiredDDLs, beforeApply, nil)
}

func (m *Migrator) ExecuteMigrationForTargetTables(tables []*Table, preHook ...string) error {
	desiredDDLs, err := m.GenerateDesiredDDLs(tables)
	if err != nil {
		return err
	}
	beforeApply := m.GenerateCreateSchemaStmt(tables)
	for _, h := range preHook {
		beforeApply += "\n" + h + ";"
	}
	return m.executeMigration(desiredDDLs, beforeApply, tables)
}

func (m *Migrator) executeMigration(desiredDDLs string, beforeApply string, onlyTables []*Table) error {
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
		if strings.Contains(err.Error(), "SSL is not enabled") && m.preferSSL {
			log.Println("[debug] ssl_mode prefer retry migrate on ssl disable")
			originalEnv := os.Getenv("PGSSLMODE")
			os.Setenv("PGSSLMODE", "disable")
			targetDB, err = postgres.NewDatabase(m.target)
			if err != nil {
				return err
			}
			os.Setenv("PGSSLMODE", originalEnv)
			currentDDLs, err = targetDB.DumpDDLs()
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("failed dump target db ddls:%w", err)
		}
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
	if len(onlyTables) == 0 {
		if err := database.RunDDLs(targetDB, ddls, false, beforeApply); err != nil {
			return fmt.Errorf("run DDLs:%w", err)
		}
		return nil
	}
	filterdDDLs := make([]string, 0)
	for _, ddl := range ddls {
		for _, t := range onlyTables {
			if strings.Contains(ddl, t.String()) {
				filterdDDLs = append(filterdDDLs, ddl)
			}
		}
	}
	if err := database.RunDDLs(targetDB, filterdDDLs, false, beforeApply); err != nil {
		return fmt.Errorf("run DDLs:%w", err)
	}
	return nil
}
