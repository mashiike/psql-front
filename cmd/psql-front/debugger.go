/*
The code in this file is partially edited from the following code.

  https://github.com/kayac/go-katsubushi/tree/master/cmd/katsubushi

  The original code license is: https://github.com/kayac/go-katsubushi/blob/master/LICENSE

  Copyright (c) 2015 KAYAC Inc.
*/

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	statsapi "github.com/fukata/golang-stats-api-handler"
)

type profConfig struct {
	enablePprof bool
	enableStats bool
	debugPort   int
}

func (pc profConfig) enabled() bool {
	return pc.enablePprof || pc.enableStats
}

func profiler(ctx context.Context, pc *profConfig) error {

	mux := http.NewServeMux()
	if pc.enablePprof {
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		log.Println("[info] enable pprof on /debug/pprof")
	}
	if pc.enableStats {
		mux.HandleFunc("/debug/stats", statsapi.Handler)
		log.Println("[info] enable stats on /debug/stats")
	}
	addr := fmt.Sprintf(":%d", pc.debugPort)
	log.Println("[info] Listening debugger on", addr)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	var serverErr error
	var wg sync.WaitGroup
	server := http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Printf("[debug][debugger] %s %s", r.Method, r.URL.String())
			mux.ServeHTTP(w, r)
		}),
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverErr = server.Serve(ln)
		log.Println("[info] debugger shutdown.")
	}()
	<-ctx.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Println("[error] failed to gracefully shutdown debugger:", err)
		server.Close()
		log.Println("[info] debugger closed immediately")
	}
	ln.Close()
	wg.Wait()
	return serverErr
}
