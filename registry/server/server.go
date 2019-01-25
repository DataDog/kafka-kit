package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DataDog/kafka-kit/kafkazk"
	pb "github.com/DataDog/kafka-kit/registry/protos"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

const (
	readRequest = iota
	writeRequest
)

// Server implements the registry APIs.
type Server struct {
	HTTPListen       string
	GRPCListen       string
	ZK               kafkazk.Handler
	Tags             *TagHandler
	readReqThrottle  RequestThrottle
	writeReqThrottle RequestThrottle
	reqID            uint64
	// For tests.
	test bool
}

// Config holds Server configurations.
type Config struct {
	HTTPListen   string
	GRPCListen   string
	ReadReqRate  int
	WriteReqRate int
	ZKTagsPrefix string

	test bool
}

// NewServer initializes a *Server.
func NewServer(c Config) (*Server, error) {
	switch {
	case c.ZKTagsPrefix == "":
		fallthrough
	case c.ReadReqRate < 1:
		fallthrough
	case c.WriteReqRate < 1:
		return nil, errors.New("invalid configuration parameter(s)")
	}

	rrt, _ := NewRequestThrottle(RequestThrottleConfig{
		Capacity: 10,
		Rate:     c.ReadReqRate,
	})

	wrt, _ := NewRequestThrottle(RequestThrottleConfig{
		Capacity: 10,
		Rate:     c.WriteReqRate,
	})

	tcfg := TagHandlerConfig{
		Prefix: c.ZKTagsPrefix,
	}

	th, _ := NewTagHandler(tcfg)
	if c.test {
		th.Store = newzkTagStorageMock()
	}

	return &Server{
		HTTPListen:       c.HTTPListen,
		GRPCListen:       c.GRPCListen,
		Tags:             th,
		readReqThrottle:  rrt,
		writeReqThrottle: wrt,
		test:             c.test,
	}, nil
}

// Run* methods take a Context for cancellation and WaitGroup
// for blocking on graceful shutdowns in main. Each call will background
// a listener (e.g. gRPC, HTTP) and a graceful shutdown procedure that's
// called when the input contex is cancelled. An error is only returned upon
// initialization. XXX(jamie) Latent errors and listener restart behavior
// should be specified.

// RunRPC runs the gRPC endpoint.
func (s *Server) RunRPC(ctx context.Context, wg *sync.WaitGroup) error {
	wg.Add(1)

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
	wg.Add(1)

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
	if s.test {
		s.ZK = &kafkazk.Mock{}
		return nil
	}

	wg.Add(1)

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
		return fmt.Errorf("failed to dial ZooKeeper in %s", zkReadyWait)
	}

	log.Printf("Connected to ZooKeeper: %s\n", c.Connect)

	// Pass the Handler to the underlying TagHandler Store
	// and call the Init procedure.
	// TODO this needs to go somewhere else.
	s.Tags.Store.(*ZKTagStorage).ZK = zk
	if err := s.Tags.Store.(*ZKTagStorage).Init(); err != nil {
		return fmt.Errorf("failed to initialize ZooKeeper TagStorage backend")
	}

	// Shutdown procedure.
	go func() {
		<-ctx.Done()
		zk.Close()
		wg.Done()
	}()

	return nil
}

// ValidateRequest takes an incoming request context, params, and request
// kind. The request is logged and checked against the appropriate request
// throttler.
func (s *Server) ValidateRequest(ctx context.Context, req interface{}, kind int) error {
	reqID := atomic.AddUint64(&s.reqID, 1)

	// Log the request.
	s.LogRequest(ctx, fmt.Sprintf("%v", req), reqID)

	var to context.Context

	// If the request context didn't have a deadline,
	// instantiate our own for the request throttle.
	if _, ok := ctx.Deadline(); ok {
		to = ctx
	} else {
		to, _ = context.WithTimeout(ctx, 500*time.Millisecond)
	}

	var err error
	defer func() {
		if err != nil {
			log.Printf("[request %d] timed out", reqID)
		}
	}()

	// Check the appropriate request throttle.
	switch kind {
	case 0:
		if err = s.readReqThrottle.Request(to); err != nil {
			return err
		}
	case 1:
		if err = s.writeReqThrottle.Request(to); err != nil {
			return err
		}
	}

	return nil
}

// LogRequest takes a request context and input parameters as a string
// and logs the request data.
func (s *Server) LogRequest(ctx context.Context, params string, reqID uint64) {
	if s.test {
		return
	}

	var requestor string
	var reqType string
	var method string

	if params == "" {
		params = "none"
	}

	// Get Peer info.
	if p, ok := peer.FromContext(ctx); ok {
		requestor = p.Addr.String()
	}

	// Get Metadata.
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if _, ok := md["grpcgateway-user-agent"]; ok {
			reqType = "http"
		} else {
			reqType = "grpc"
		}
	}

	// Get the gRPC method.
	method, _ = grpc.Method(ctx)

	log.Printf("[request %d] requestor:%s type:%s method:%s params:%s",
		reqID, requestor, reqType, method, params)
}
