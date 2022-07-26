package psqlfront

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	psqlwire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

type ProxyConnection struct {
	id string
	*pgx.Conn
}

func (conn *ProxyConnection) ID() string {
	return conn.id
}

func (conn *ProxyConnection) Close(ctx context.Context) error {
	return conn.Conn.Close(ctx)
}

type Server struct {
	s                        *psqlwire.Server
	db                       *pgxpool.Pool
	cacheTTL                 map[string]time.Duration
	origins                  map[string]Origin
	originIDsByTable         map[string]string
	tables                   map[string]*Table
	migrator                 *Migrator
	proxyConnections         map[string]*ProxyConnection
	proxyConnectionGenerator func(ctx context.Context, id string) (*ProxyConnection, error)
}

func New(ctx context.Context, cfg *Config) (*Server, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.CacheDatabase.DSN())
	if err != nil {
		return nil, fmt.Errorf("unable to parse DATABASE_URL: %w", err)
	}
	db, err := pgxpool.ConnectConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}
	server := &Server{
		db:               db,
		cacheTTL:         make(map[string]time.Duration, len(cfg.Origins)),
		origins:          make(map[string]Origin, len(cfg.Origins)),
		originIDsByTable: make(map[string]string),
		tables:           make(map[string]*Table),
		migrator:         NewMigrator(cfg.CacheDatabase),
		proxyConnections: make(map[string]*ProxyConnection),
		proxyConnectionGenerator: func(ctx context.Context, id string) (*ProxyConnection, error) {
			conn, err := pgx.Connect(ctx, cfg.CacheDatabase.DSN())
			if err != nil {
				return nil, err
			}
			log.Println("[info] new connection: ", id)
			return &ProxyConnection{
				id:   id,
				Conn: conn,
			}, nil
		},
	}
	serverOpts := []psqlwire.OptionFn{
		psqlwire.SimpleQuery(server.handleSimpleQuery),
		psqlwire.CloseConn(func(ctx context.Context) error {
			var mark string
			if proxyConnection, err := server.ProxyConnection(ctx); err == nil {
				mark = fmt.Sprintf(": %s", proxyConnection.ID())
				if err := proxyConnection.Close(ctx); err != nil {
					return fmt.Errorf("proxy connection close:%w", err)
				}
			}
			log.Printf("[info] close connection%s", mark)
			return nil
		}),
		psqlwire.TerminateConn(func(ctx context.Context) error {
			var mark string
			if proxyConnection, err := server.ProxyConnection(ctx); err == nil {
				mark = fmt.Sprintf(": %s", proxyConnection.ID())
				if err := proxyConnection.Close(ctx); err != nil {
					return fmt.Errorf("proxy connection close:%w", err)
				}
			}
			log.Printf("[info] terminate connection%s", mark)
			return nil
		}),
	}
	if len(cfg.Certificates) > 0 {
		log.Println("[info] use TLS")
		certs := make([]tls.Certificate, 0, len(cfg.Certificates))
		for _, certCfg := range cfg.Certificates {
			cert, err := tls.LoadX509KeyPair(certCfg.Cert, certCfg.Key)
			if err != nil {
				return nil, err
			}
			certs = append(certs, cert)
		}
		serverOpts = append(serverOpts, psqlwire.Certificates(certs))
	}
	server.s, err = psqlwire.NewServer(serverOpts...)
	if err != nil {
		return nil, err
	}
	server.s.Auth = psqlwire.ClearTextPassword(func(username, password string) (bool, error) {
		if username != cfg.CacheDatabase.Username || password != cfg.CacheDatabase.Password {
			return false, nil
		}
		return true, nil
	})

	for _, origin := range cfg.Origins {
		server.cacheTTL[origin.ID] = *origin.TTL
		o, err := origin.NewOrigin()
		if err != nil {
			return nil, fmt.Errorf("origin `%s` initialize: %w", origin.ID, err)
		}
		server.origins[origin.ID] = o
	}
	return server, nil
}

func (server *Server) RunWithContext(ctx context.Context, address string) error {
	tables := make([]*Table, 0, len(server.origins))
	for _, origin := range server.origins {
		t, err := origin.GetTables(ctx)
		if err != nil {
			return fmt.Errorf("origin_id `%s` get tables:%w", origin.ID(), err)
		}
		for _, table := range t {
			server.originIDsByTable[table.String()] = origin.ID()
			server.tables[table.String()] = table
			log.Printf("[debug] %s: %d columns", table.String(), len(table.Columns))
		}
		tables = append(tables, t...)
	}
	if err := server.migrator.ExecuteMigration(tables); err != nil {
		return fmt.Errorf("execute migration:%w", err)
	}
	if err := server.analezeTables(ctx, tables); err != nil {
		return fmt.Errorf("execute initial analyze:%w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("[info] start shutdown...")
		server.Close()
	}()

	log.Printf("[info] PostgreSQL server is up and running at [%s]", address)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer func() {
		if err := listener.Close(); err != nil {
			log.Println("[debug] on listener close:", err)
		}
	}()
	err = server.s.Serve(listener)

	wg.Wait()
	log.Println("[info] shutdown.")
	if err != nil {
		var oe *net.OpError
		if errors.As(err, &oe) {
			if oe.Op == "accept" {
				return nil
			}
		}
		return err
	}
	return nil
}

func (server *Server) handleSimpleQuery(ctx context.Context, query string, writer psqlwire.DataWriter) error {
	log.Println("[debug] incoming SQL:", query)
	proxyConnection, err := server.ProxyConnection(ctx)
	if err != nil {
		return err
	}
	connectionID := proxyConnection.ID()
	log.Printf("[info][%s] incoming SQL: %s", connectionID, query)
	tables, isQueryRows, err := AnalyzeQuery(query)
	if err != nil {
		return err
	}
	log.Printf("[info][%s] referenced tables: [%s]", connectionID, strings.Join(lo.Map(tables, func(table *Table, _ int) string {
		return table.String()
	}), ", "))

	if err := server.controlCache(ctx, query, tables); err != nil {
		return fmt.Errorf("control cache failed: %w", err)
	}

	if err := server.executeQuery(ctx, query, isQueryRows, writer); err != nil {
		return err
	}
	log.Printf("[info][%s] success SQL", connectionID)
	return nil
}

func (server *Server) executeQuery(ctx context.Context, query string, isQueryRows bool, writer psqlwire.DataWriter) error {
	proxyConnection, err := server.ProxyConnection(ctx)
	if err != nil {
		return fmt.Errorf("get proxy connection:%w", err)
	}
	if !isQueryRows {
		tag, err := proxyConnection.Exec(ctx, query)
		if err != nil {
			log.Println("[info] failed SQL query:", err)
			return errors.New(strings.TrimSpace(strings.TrimPrefix(err.Error(), "ERROR:")))
		}
		return writer.Complete(tag.String())
	}

	rows, err := proxyConnection.Query(ctx, query)
	if err != nil {
		log.Println("[info] failed SQL query:", err)
		return errors.New(strings.TrimSpace(strings.TrimPrefix(err.Error(), "ERROR:")))
	}
	defer rows.Close()
	fieldDescriptions := rows.FieldDescriptions()
	columns := make(psqlwire.Columns, 0, len(fieldDescriptions))
	for _, fieldDescription := range fieldDescriptions {
		column := psqlwire.Column{
			Table:  int32(fieldDescription.TableOID),
			Name:   string(fieldDescription.Name),
			AttrNo: int16(fieldDescription.TableAttributeNumber),
			Oid:    oid.Oid(fieldDescription.DataTypeOID),
			Width:  fieldDescription.DataTypeSize,
		}
		if fieldDescription.TypeModifier >= 0 {
			column.TypeModifier = fieldDescription.TypeModifier
		}
		columns = append(columns, column)
	}
	if err := writer.Define(columns); err != nil {
		return fmt.Errorf("define: %w", err)
	}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("row values: %w", err)
		}
		if err := writer.Row(values); err != nil {
			return fmt.Errorf("wirter row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}

	return writer.Complete("OK")
}

var cacheLifecycleTable = &Table{
	SchemaName: "psqlfront",
	RelName:    "cache",
}

func (server *Server) analezeTables(ctx context.Context, tables []*Table) error {
	var mark string
	if proxyConnection, err := server.ProxyConnection(ctx); err == nil {
		mark = fmt.Sprintf("[%s]", proxyConnection.ID())
	}
	if len(tables) == 0 {
		return nil
	}
	sql := "ANALYZE " + strings.Join(lo.Map(tables, func(table *Table, _ int) string {
		return table.String()
	}), ", ") + ";"
	log.Printf("[info]%s execute: %s", mark, sql)
	_, err := server.db.Exec(ctx, sql)
	return err
}

func (server *Server) controlCache(ctx context.Context, query string, refarencedTables []*Table) error {
	var mark string
	if proxyConnection, err := server.ProxyConnection(ctx); err == nil {
		mark = fmt.Sprintf("[%s]", proxyConnection.ID())
	}
	tables := make([]*Table, 0, len(refarencedTables))
	for _, table := range refarencedTables {
		if table.String() == cacheLifecycleTable.String() {
			continue
		}
		if table.SchemaName == "pg_catalog" || table.SchemaName == "information_schema" {
			continue
		}
		t, ok := server.tables[table.String()]
		if !ok {
			continue
		}
		tables = append(tables, t)
	}
	if len(tables) == 0 {
		log.Printf("[info]%s only system tables or no managed by psqlfront, no check cache", mark)
		return nil
	}
	cacheInfo, err := server.getCacheInfo(ctx, tables)
	if err != nil {
		return fmt.Errorf("get cache info:%w", err)
	}
	noHitTables := lo.Filter(tables, func(t *Table, _ int) bool {
		_, ok := cacheInfo[t.String()]
		return !ok
	})
	if len(noHitTables) == 0 {
		log.Printf("[info]%s all tables cache hit", mark)
		return nil
	}
	log.Printf("[info]%s cache no hit tables: [%s]", mark, strings.Join(lo.Map(noHitTables, func(table *Table, _ int) string {
		return table.String()
	}), ", "))
	eg, egctx := errgroup.WithContext(ctx)
	for _, noHitTable := range noHitTables {
		t := noHitTable
		eg.Go(func() error {
			tx, err := server.db.Begin(egctx)
			log.Printf("[debug] start `%s` tx", t.String())
			if err != nil {
				return fmt.Errorf("start tx:%w", err)
			}
			var commited bool
			var rollbacked bool
			defer func() {
				if !commited && !rollbacked {
					if err := tx.Rollback(ctx); err != nil {
						log.Printf("[warn]%s %s tx rollback failed: %v", mark, t.String(), err)
					} else {
						log.Printf("[debug]%s %s tx rollback", mark, t.String())
					}
				}
				log.Printf("[debug]%s end `%s` tx", mark, t.String())
			}()
			if err := server.refreshCache(egctx, tx, t); err != nil {
				log.Printf("[warn]%s %s can not refresh cache: %v", mark, t, err)
				var onfe *OriginNotFoundError
				if !errors.As(err, &onfe) {
					return fmt.Errorf("refresh cache:%w", err)
				}
				if err := tx.Rollback(ctx); err != nil {
					log.Printf("[warn]%s %s tx rollback failed: %v", mark, t.String(), err)
				} else {
					log.Printf("[debug]%s %s tx rollback", mark, t.String())
				}
				rollbacked = true
				return nil
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit tx:%w", err)
			}
			commited = true
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("refresh cache failed:%w", err)
	}
	if err := server.analezeTables(ctx, noHitTables); err != nil {
		return fmt.Errorf("execute noHitTables analyze:%w", err)
	}
	return nil
}

type OriginNotFoundError struct {
	err error
}

func (onfe *OriginNotFoundError) Error() string {
	return onfe.err.Error()
}

func (onfe *OriginNotFoundError) Unwrap() error {
	return onfe.err
}

func WrapOriginNotFoundError(err error) *OriginNotFoundError {
	return &OriginNotFoundError{err: err}
}

type CacheInfo struct {
	SchemaName, TableName, OriginID string
	CachedAt, ExpiredAt             time.Time
}

var psqlQueryBuilder = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func (server *Server) getCacheInfo(ctx context.Context, tables []*Table) (map[string]*CacheInfo, error) {
	var mark string
	if proxyConnection, err := server.ProxyConnection(ctx); err == nil {
		mark = fmt.Sprintf("[%s]", proxyConnection.ID())
	}
	cond := make(sq.Or, 0, len(tables))
	for _, table := range tables {
		cond = append(cond, sq.Eq{
			"schema_name": table.SchemaName,
			"table_name":  table.RelName,
		})
	}
	sql, args, err := psqlQueryBuilder.Select(
		"schema_name",
		"table_name",
		"origin_id",
		"cached_at",
		"expired_at",
	).From(cacheLifecycleTable.String()).Where(sq.And{cond, sq.Expr("expired_at > NOW()")}).ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query:%w", err)
	}
	log.Printf("[debug]%s execute: %s; %v", mark, sql, args)
	rows, err := server.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]*CacheInfo)
	for rows.Next() {
		var cacheInfo CacheInfo
		if err := rows.Scan(
			&cacheInfo.SchemaName,
			&cacheInfo.TableName,
			&cacheInfo.OriginID,
			&cacheInfo.CachedAt,
			&cacheInfo.ExpiredAt,
		); err != nil {
			return nil, fmt.Errorf("row scan: %w", err)
		}
		log.Printf(
			"[debug]%s origin_id:%s schema_name:%s table_name:%s cached_at:%s, exired_at:%s", mark,
			cacheInfo.OriginID, cacheInfo.SchemaName, cacheInfo.TableName, cacheInfo.CachedAt.Format(time.RFC3339), cacheInfo.ExpiredAt.Format(time.RFC3339),
		)
		t := &Table{
			SchemaName: cacheInfo.SchemaName,
			RelName:    cacheInfo.TableName,
		}
		result[t.String()] = &cacheInfo
	}
	return result, nil
}

type cacheWriter struct {
	tx    pgx.Tx
	table *Table
}

func (w *cacheWriter) AppendRows(ctx context.Context, rows [][]interface{}) error {

	columns := lo.Map(w.table.Columns, func(c *Column, _ int) string {
		return c.Name
	})
	q := psqlQueryBuilder.Insert(w.table.String()).Columns(columns...)

	for _, row := range rows {
		if len(row) != len(columns) {
			return fmt.Errorf("expected columns %d, acutal columns %d", len(columns), len(row))
		}
		q = q.Values(row...)
	}
	sql, args, err := q.ToSql()
	if err != nil {
		return fmt.Errorf("build insert into `%s` query:%w", w.table, err)
	}
	log.Printf("[debug] execute: %s; %v", sql, args)
	tag, err := w.tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("execute insert into `%s` query:%w", w.table, err)
	}
	log.Printf("[info] %s %d rows inserted", w.table, tag.RowsAffected())
	return nil
}

func (server *Server) refreshCache(ctx context.Context, tx pgx.Tx, table *Table) error {
	log.Printf("[debug] refresh target %s: %d columns", table.String(), len(table.Columns))
	originID, ok := server.originIDsByTable[table.String()]
	if !ok {
		return WrapOriginNotFoundError(fmt.Errorf("table %s not found", table))
	}
	origin, ok := server.origins[originID]
	if !ok {
		return WrapOriginNotFoundError(fmt.Errorf("origin %s not found", table))
	}
	sql, args, err := psqlQueryBuilder.Delete(table.String()).ToSql()
	if err != nil {
		return fmt.Errorf("build delete from `%s` query:%w", table, err)
	}
	log.Printf("[debug] execute: %s; %v", sql, args)
	tag, err := tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("execute delete from `%s` query:%w", table, err)
	}
	log.Printf("[info] %s %d rows deleted", table, tag.RowsAffected())
	log.Printf("[info] get rows from origin `%s`", originID)
	err = origin.GetRows(ctx, &cacheWriter{
		tx:    tx,
		table: table,
	}, table)
	if err != nil {
		return fmt.Errorf("origin %s, table %s get rows:%w", originID, table, err)
	}
	ttl, ok := server.cacheTTL[originID]
	if !ok {
		return fmt.Errorf("%s's ttl not found", originID)
	}
	sql, args, err = psqlQueryBuilder.Insert(cacheLifecycleTable.String()).Columns(
		"schema_name",
		"table_name",
		"origin_id",
		"cached_at",
		"expired_at",
	).Values(
		table.SchemaName,
		table.RelName,
		originID,
		sq.Expr("NOW()"),
		sq.Expr(fmt.Sprintf("NOW() + interval '%d seconds'", int64(ttl.Seconds()))),
	).Suffix(
		"ON CONFLICT (schema_name, table_name) DO UPDATE SET origin_id=EXCLUDED.origin_id, cached_at=EXCLUDED.cached_at,expired_at=EXCLUDED.expired_at",
	).ToSql()
	if err != nil {
		return fmt.Errorf("build cache upsert `%s` query:%w", table, err)
	}
	log.Printf("[debug] execute: %s; %v", sql, args)
	tag, err = tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("execute cache upsert `%s` query:%w", table, err)
	}
	log.Printf("[info] %s %s", cacheLifecycleTable.String(), tag)
	return nil
}

func (server *Server) Close() error {
	closedCh := make(chan struct{})
	go func() {
		if err := server.s.Close(); err != nil {
			log.Println("[error] on close:", err)
		}
		close(closedCh)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
		log.Println("[warn] close timeout")
		return ctx.Err()
	case <-closedCh:
		return nil
	}
}

var parameterConnectionStatus psqlwire.ParameterStatus = "__psql_front_connection_id"

func (server *Server) ProxyConnection(ctx context.Context) (*ProxyConnection, error) {
	params := psqlwire.ServerParameters(ctx)
	if params == nil {
		return nil, errors.New("server params is nil")
	}
	connectionID, ok := params[parameterConnectionStatus]
	if ok {
		if conn, ok := server.proxyConnections[connectionID]; ok {
			return conn, nil
		}
		return nil, fmt.Errorf("proxy conenction not found for `%s`", connectionID)
	}
	uuidObj, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("generate uuid:%w", err)
	}
	connectionID = uuidObj.String()
	proxyConnection, err := server.proxyConnectionGenerator(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("generate proxy connection:%w", err)
	}
	server.proxyConnections[connectionID] = proxyConnection
	params[parameterConnectionStatus] = connectionID
	return proxyConnection, nil
}
