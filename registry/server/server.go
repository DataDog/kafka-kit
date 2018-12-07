package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/DataDog/kafka-kit/kafkazk"
	pb "github.com/DataDog/kafka-kit/registry/protos"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

// Server implements the registry APIs.
type Server struct {
	HTTPListen string
	GRPCListen string
	ZK         kafkazk.Handler
}

// ServerConfig holds Server configurations.
type ServerConfig struct {
	HTTPListen string
	GRPCListen string
}

// NewServer initializes a *Server.
func NewServer(c ServerConfig) *Server {
	return &Server{
		HTTPListen: c.HTTPListen,
		GRPCListen: c.GRPCListen,
	}
}

// Run* methods take a Context for cancellation and WaitGroup
// for blocking on graceful shutdowns in main. Each call will background
// a listener (e.g. gRPC, HTTP) and a graceful shutdown procedure that's
// called when the input contex is cancelled. An error is only returned upon
// initialization. XXX(jamie) Latent errors and listener restart behavior
// should be specified.

// RunRPC runs the gRPC endpoint.
func (s *Server) RunRPC(ctx context.Context, wg *sync.WaitGroup) error {
	l, err := net.Listen("tcp", s.GRPCListen)
	if err != nil {
		return err
	}

	srvr := grpc.NewServer()
	pb.RegisterRegistryServer(srvr, s)

	// Shutdown procedure.
	go func() {
		<-ctx.Done()
		log.Println("Shutting down gRPC listener")

		srvr.GracefulStop()

		if err := l.Close(); err != nil {
			log.Println(err)
		}

		wg.Done()
	}()

	// Background the listener.
	go func() {
		log.Printf("gRPC up: %s\n", s.GRPCListen)
		if err := srvr.Serve(l); err != nil {
			log.Println(err)
		}
	}()

	return nil
}

// RunHTTP runs the HTTP endpoint.
func (s *Server) RunHTTP(ctx context.Context, wg *sync.WaitGroup) error {
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}

	err := pb.RegisterRegistryHandlerFromEndpoint(ctx, mux, s.GRPCListen, opts)
	if err != nil {
		return err
	}

	srvr := &http.Server{
		Addr:    s.HTTPListen,
		Handler: mux,
	}

	// Shutdown procedure.
	go func() {
		<-ctx.Done()
		log.Println("Shutting down HTTP listener")

		if err := srvr.Shutdown(ctx); err != nil {
			log.Println(err)
		}

		wg.Done()
	}()

	// Background the listener.
	go func() {
		log.Printf("HTTP up: %s\n", s.HTTPListen)
		if err := srvr.ListenAndServe(); err != http.ErrServerClosed {
			log.Println(err)
		}
	}()

	return nil
}

// DialZK takes a Context, WaitGroup and *kafkazk.Config and initializes
// a kafkazk.Handler. A background shutdown procedure is called when the
// context is cancelled.
func (s *Server) DialZK(ctx context.Context, wg *sync.WaitGroup, c *kafkazk.Config) error {
	// Init.
	zk, err := kafkazk.NewHandler(c)
	if err != nil {
		return err
	}

	s.ZK = zk

	// Test readiness.
	zkReadyWait := 250 * time.Millisecond
	time.Sleep(zkReadyWait)

	if !zk.Ready() {
		return fmt.Errorf("Failed to dial ZooKeeper in %s\n", zkReadyWait)
	}

	log.Printf("Connected to ZooKeeper: %s\n", c.Connect)

	// Shutdown procedure.
	go func() {
		<-ctx.Done()
		zk.Close()
		wg.Done()
	}()

	return nil
}
