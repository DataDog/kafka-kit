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

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/DataDog/kafka-kit/v3/cluster"
	zklocking "github.com/DataDog/kafka-kit/v3/cluster/zookeeper"
	"github.com/DataDog/kafka-kit/v3/kafkaadmin"
	"github.com/DataDog/kafka-kit/v3/kafkazk"
	pb "github.com/DataDog/kafka-kit/v3/registry/registry"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
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
	pb.UnimplementedRegistryServer
	Locking               cluster.Lock
	HTTPListen            string
	GRPCListen            string
	ZK                    kafkazk.Handler
	kafkaadmin            kafkaadmin.KafkaAdmin
	Tags                  *TagHandler
	defaultRequestTimeout time.Duration
	readReqThrottle       RequestThrottle
	writeReqThrottle      RequestThrottle
	reqID                 uint64
	kafkaconsumer         *kafka.Consumer
	// For tests.
	test bool
}

// Config holds Server configurations.
type Config struct {
	HTTPListen                 string
	GRPCListen                 string
	ReadReqRate                int
	WriteReqRate               int
	ZKTagsPrefix               string
	DefaultRequestTimeout      time.Duration
	TagCleanupFrequencyMinutes int
	TagAllowedStalenessMinutes int

	test bool
}

// NewServer initializes a *Server.
func NewServer(c Config) (*Server, error) {
	switch {
	case c.ZKTagsPrefix == "",
		c.ReadReqRate < 1,
		c.WriteReqRate < 1:
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

	return &Server{
		Locking:               dummyLock{},
		HTTPListen:            c.HTTPListen,
		GRPCListen:            c.GRPCListen,
		Tags:                  th,
		defaultRequestTimeout: c.DefaultRequestTimeout,
		readReqThrottle:       rrt,
		writeReqThrottle:      wrt,
		test:                  c.test,
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

// runTagCleanup starts a background process deleting stale tags.
func (s *Server) RunTagCleanup(ctx context.Context, wg *sync.WaitGroup, c Config) error {
	wg.Add(1)
	tc := TagCleaner{}

	// Shutdown procedure.
	go func() {
		<-ctx.Done()
		tc.running = false
		wg.Done()
	}()

	// Background the tag cleanup mark & sweep
	go func() {
		tc.RunTagCleanup(s, ctx, c)
	}()

	return nil
}

// InitKafkaAdmin takes a Context, WaitGroup and an admin.Config and initializes
// an admin.Client. A background shutdown procedure is called when the
// context is cancelled.
func (s *Server) InitKafkaAdmin(ctx context.Context, wg *sync.WaitGroup, cfg kafkaadmin.Config) error {
	if s.test {
		return nil
	}

	wg.Add(1)

	k, err := kafkaadmin.NewClient(
		kafkaadmin.Config{
			BootstrapServers: cfg.BootstrapServers,
			SSLCALocation:    cfg.SSLCALocation,
			SecurityProtocol: cfg.SecurityProtocol,
			SASLMechanism:    cfg.SASLMechanism,
			SASLUsername:     cfg.SASLUsername,
			SASLPassword:     cfg.SASLPassword,
		})
	if err != nil {
		return err
	}

	s.kafkaadmin = k
	log.Printf("KafkaAdmin connected to bootstrap servers: %s\n", cfg.BootstrapServers)

	// Shutdown procedure.
	go func() {
		<-ctx.Done()
		s.kafkaadmin.Close()
		wg.Done()
	}()

	return nil
}

// InitKafkaConsumer takes a Context, WaitGroup and an admin.Config and initializes
// a kafka.Consumer. A background shutdown procedure is called when the context is cancelled.
func (s *Server) InitKafkaConsumer(ctx context.Context, wg *sync.WaitGroup, cfg kafkaadmin.Config) error {
	if s.test {
		return nil
	}

	wg.Add(1)

	k, err := kafkaadmin.NewConsumer(
		kafkaadmin.Config{
			BootstrapServers: cfg.BootstrapServers,
			GroupId:          "registry",
			SSLCALocation:    cfg.SSLCALocation,
			SecurityProtocol: cfg.SecurityProtocol,
			SASLMechanism:    cfg.SASLMechanism,
			SASLUsername:     cfg.SASLUsername,
			SASLPassword:     cfg.SASLPassword,
		})
	if err != nil {
		return err
	}

	s.kafkaconsumer = k
	log.Printf("KafkaConsumer connected to bootstrap servers: %s\n", cfg.BootstrapServers)

	// Shutdown procedure.
	go func() {
		<-ctx.Done()
		s.kafkaconsumer.Close()
		wg.Done()
	}()

	return nil
}

// EnablingLocking uses distributed locking for write operations.
func (s *Server) EnablingLocking(c *kafkazk.Config) error {
	cfg := zklocking.ZooKeeperLockConfig{
		Address:  c.Connect,
		Path:     "/registry/locks",
		OwnerKey: "reqID",
	}

	zkl, err := zklocking.NewZooKeeperLock(cfg)
	if err != nil {
		return fmt.Errorf("failed to initialize ZooKeeper locking backend")
	}

	log.Printf("Using ZooKeeper based distributed locking")
	s.Locking = zkl

	return nil
}

// DialZK takes a Context, WaitGroup and *kafkazk.Config and initializes
// a kafkazk.Handler. A background shutdown procedure is called when the
// context is cancelled.
func (s *Server) DialZK(ctx context.Context, wg *sync.WaitGroup, c *kafkazk.Config) error {
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
// throttler. If the incoming context did not have a deadline set, the server
// a derived context is created with the server default timeout. The child
// context and error are returned.
func (s *Server) ValidateRequest(ctx context.Context, req interface{}, kind int) (context.Context, context.CancelFunc, error) {
	// Check if this context has already been seen. If so, it's likely that
	// one gRPC call is internally calling another and visiting ValidateRequest
	// multiple times. In this case, we don't need to do further rate limiting,
	// logging, and other steps.
	if _, seen := ctx.Value("reqID").(uint64); seen {
		return ctx, nil, nil
	}

	reqID := atomic.AddUint64(&s.reqID, 1)

	// Log the request.
	s.LogRequest(ctx, fmt.Sprintf("%v", req), reqID)

	var cCtx context.Context
	var cancel context.CancelFunc

	// Check if the incoming context has a deadline set.
	configuredDeadline, deadlineSet := ctx.Deadline()
	maxDeadline := s.defaultRequestTimeout * 3
	deadlineLimit := time.Now().Add(maxDeadline)

	switch {
	// No deadline set, use our default.
	case !deadlineSet:
		cCtx, cancel = context.WithTimeout(ctx, s.defaultRequestTimeout)
	// A deadline was set, but it's longer than we permit.
	case configuredDeadline.After(deadlineLimit):
		cCtx, cancel = context.WithTimeout(ctx, maxDeadline)
	// An acceptable deadline was configured.
	default:
		cCtx = ctx
	}

	cCtx = context.WithValue(cCtx, "reqID", reqID)

	var err error
	defer func() {
		if err != nil {
			log.Printf("[request %d] timed out", reqID)
			cancel()
		}
	}()

	// Check the appropriate request throttle.
	switch kind {
	case 0:
		if err = s.readReqThrottle.Request(cCtx); err != nil {
			return nil, nil, err
		}
	case 1:
		if err = s.writeReqThrottle.Request(cCtx); err != nil {
			return nil, nil, err
		}
	}

	return cCtx, cancel, nil
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

type dummyLock struct{}

func (dl dummyLock) Lock(_ context.Context) error   { return nil }
func (dl dummyLock) Unlock(_ context.Context) error { return nil }
func (dl dummyLock) Owner() interface{}             { return nil }
