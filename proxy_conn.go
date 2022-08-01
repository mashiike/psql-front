package psqlfront

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/jackc/pgproto3/v2"
	"golang.org/x/sync/errgroup"
)

type ProxyConnOptions struct {
	tlsConfig              *tls.Config
	onQueryReceivedHandler func(ctx context.Context, query string, isPreparedStmt bool) error
}

type ProxyConn struct {
	backend  *pgproto3.Backend
	frontend *pgproto3.Frontend
	opts     *ProxyConnOptions
	client   net.Conn
	upstream net.Conn
}

func WithProxyConnTLS(tlsConfig *tls.Config) func(opts *ProxyConnOptions) {
	return func(opts *ProxyConnOptions) {
		opts.tlsConfig = tlsConfig
	}
}

func WithProxyConnOnQueryReceived(handler func(ctx context.Context, query string, isPreparedStmt bool) error) func(opts *ProxyConnOptions) {
	return func(opts *ProxyConnOptions) {
		opts.onQueryReceivedHandler = handler
	}
}

func NewProxyConn(client net.Conn, upstream net.Conn, optFns ...func(opts *ProxyConnOptions)) (*ProxyConn, error) {
	conn := &ProxyConn{
		backend:  pgproto3.NewBackend(pgproto3.NewChunkReader(client), client),
		frontend: pgproto3.NewFrontend(pgproto3.NewChunkReader(upstream), upstream),
		opts:     &ProxyConnOptions{},
		client:   client,
		upstream: upstream,
	}
	for _, optFn := range optFns {
		optFn(conn.opts)
	}
	return conn, nil
}

func (conn *ProxyConn) Run(ctx context.Context) error {
	defer conn.client.Close()
	defer conn.upstream.Close()
	remoteAddr := conn.client.RemoteAddr()
	log.Printf("[debug][%s] start proxy connection", remoteAddr)
	defer log.Printf("[debug][%s] end proxy connection", remoteAddr)
	startupMessage, err := conn.backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("ReceiveStartupMessage:%w", err)
	}
	log.Printf("[debug][%s] ReceiveStartupMessage", remoteAddr)
	switch startupMessage.(type) {
	case *pgproto3.SSLRequest:
		log.Printf("[debug][%s] SSLRequest", remoteAddr)
		if conn.opts.tlsConfig != nil {
			_, err := conn.client.Write([]byte("S"))
			if err != nil {
				return fmt.Errorf("send tls support:%w", err)
			}
			log.Printf("[debug][%s] suport ssl", remoteAddr)
			tlsConn := tls.Server(conn.client, conn.opts.tlsConfig)
			conn.backend = pgproto3.NewBackend(pgproto3.NewChunkReader(tlsConn), tlsConn)
			conn.client = tlsConn
			startupMessage, err = conn.backend.ReceiveStartupMessage()
			if err != nil {
				return fmt.Errorf("tls ReceiveStartupMessage:%w", err)
			}
		} else {
			log.Printf("[debug][%s] can not use ssl", remoteAddr)
			_, err := conn.client.Write([]byte("N"))
			if err != nil {
				return fmt.Errorf("send tls not support:%w", err)
			}
		}
	case *pgproto3.GSSEncRequest:
		log.Printf("[debug][%s] can not use gss enc", remoteAddr)
		_, err := conn.client.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("send gss enc not support:%w", err)
		}
	}
	if startupMessage, ok := startupMessage.(*pgproto3.StartupMessage); ok {
		var builder strings.Builder
		fmt.Fprintf(&builder, "protocol_version:%d", startupMessage.ProtocolVersion)
		for key, value := range startupMessage.Parameters {
			fmt.Fprintf(&builder, " %s:%s", key, value)
		}
		log.Printf("[info][%s] %s", remoteAddr, builder.String())
	}
	log.Printf("[debug][%s] send startup message to upstream", remoteAddr)
	if err := conn.frontend.Send(startupMessage); err != nil {
		return fmt.Errorf("frontend send startup message:%w", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	eg, cctx := errgroup.WithContext(cctx)
	eg.Go(func() error {
		defer cancel()
		for {
			select {
			case <-cctx.Done():
				return nil
			default:
			}
			fm, err := conn.backend.Receive()
			if err != nil {
				return fmt.Errorf("receive message from client:%w", err)
			}
			switch fm := fm.(type) {
			case *pgproto3.Query:
				log.Printf("[info][%s] receive message from client: incoming SQL: %s", remoteAddr, fm.String)
				if conn.opts.onQueryReceivedHandler != nil {
					if err := conn.opts.onQueryReceivedHandler(cctx, fm.String, false); err != nil {
						return fmt.Errorf("on query recived:%w", err)
					}
				}
			case *pgproto3.Parse:
				log.Printf("[info][%s] receive message from client: parse SQL: %s name=%s", remoteAddr, fm.Query, fm.Name)
				if conn.opts.onQueryReceivedHandler != nil {
					if err := conn.opts.onQueryReceivedHandler(cctx, fm.Query, true); err != nil {
						return fmt.Errorf("on query recived:%w", err)
					}
				}
			case *pgproto3.Describe:
				log.Printf("[debug][%s] receive message from client: describe: %s type='%c'", remoteAddr, fm.Name, fm.ObjectType)
			case *pgproto3.Bind:
				log.Printf("[debug][%s] receive message from client: bind: %s", remoteAddr, fm.PreparedStatement)
			case *pgproto3.Execute:
				log.Printf("[debug][%s] receive message from client: execute: %s max_rows=%d", remoteAddr, fm.Portal, fm.MaxRows)
			case *pgproto3.Terminate:
				log.Printf("[debug][%s] receive message from client: connection terminate", remoteAddr)
				return conn.frontend.Send(fm)
			default:
				log.Printf("[debug][%s] receive message from client: %T", remoteAddr, fm)
			}
			err = conn.frontend.Send(fm)
			if err != nil {
				return fmt.Errorf("send message to upstream:%w", err)
			}
		}
	})
	eg.Go(func() error {
		defer cancel()
		for {
			select {
			case <-cctx.Done():
				return nil
			default:
			}
			bm, err := conn.frontend.Receive()
			if err != nil {
				return fmt.Errorf("receive message from upstream:%w", err)
			}
			conn.backend.SetAuthType(conn.frontend.GetAuthType())
			switch bm := bm.(type) {
			default:
				log.Printf("[debug][%s] receive message from upstream: %T", remoteAddr, bm)
			}
			err = conn.backend.Send(bm)
			if err != nil {

				return fmt.Errorf("send message to client:%w", err)

			}
		}
	})
	go func() error {
		<-ctx.Done()
		cancel()
		conn.client.Close()
		conn.upstream.Close()
		return nil
	}()
	err = eg.Wait()
	cancel()
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		return err
	}
	return nil
}
