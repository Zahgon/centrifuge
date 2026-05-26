package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge/_examples/unidirectional_grpc/clientproto"

	"github.com/centrifugal/centrifuge"
	"google.golang.org/grpc"
)

var (
	httpPort = flag.Int("http_port", 8000, "Port to bind HTTP server to")
	grpcPort = flag.Int("grpc_port", 10000, "Port to bind GRPC server to")
	redis    = flag.Bool("redis", false, "Use Redis")
)

func grpcAuthInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	_ = "STUB: not implemented"
	// You probably want to authenticate user by information included in stream metadata.
	// meta, ok := metadata.FromIncomingContext(ss.Context())
	// But here we skip it for simplicity and just always authenticate user with ID 42.
	return nil
}

// GRPC has no builtin method to add data to context so here we use small
// wrapper over ServerStream.

// WrappedServerStream is a thin wrapper around grpc.ServerStream that allows modifying context.
// This can be replaced by analogue from github.com/grpc-ecosystem/go-grpc-middleware
// package - https://github.com/grpc-ecosystem/go-grpc-middleware/blob/master/wrappers.go.
// You most probably will have dependency to it in your application as it has lots of
// useful features to deal with GRPC.
type WrappedServerStream struct {
	grpc.ServerStream
	// WrappedContext is the wrapper's own Context. You can assign it.
	WrappedContext context.Context
}

// Context returns the wrapper's WrappedContext, overwriting the nested grpc.ServerStream.Context()
func (w *WrappedServerStream) Context() context.Context {
	_ = "STUB: not implemented"
	return *

	// WrapServerStream returns a ServerStream that has the ability to overwrite context.
	new(context.Context)
}

func WrapServerStream(stream grpc.ServerStream) *WrappedServerStream {
	_ = "STUB: not implemented"
	return nil
}

func waitExitSignal(n *centrifuge.Node, server *grpc.Server) { _ = "STUB: not implemented"; return }

// RegisterGRPCServerClient ...
func RegisterGRPCServerClient(n *centrifuge.Node, server *grpc.Server, config GRPCClientServiceConfig) error {
	_ = "STUB: not implemented"
	return nil
}

// GRPCClientServiceConfig for GRPC client Service.
type GRPCClientServiceConfig struct{}

// GRPCClientService can work with client GRPC connections.
type grpcClientService struct {
	clientproto.UnimplementedCentrifugeUniServer
	config GRPCClientServiceConfig
	node   *centrifuge.Node
}

// newGRPCClientService creates new Service.
func newGRPCClientService(n *centrifuge.Node, c GRPCClientServiceConfig) *grpcClientService {
	_ = "STUB: not implemented"
	return nil
}

// Consume is a unidirectional server->client stream with real-time data.
func (s *grpcClientService) Consume(req *clientproto.ConnectRequest, stream clientproto.CentrifugeUni_ConsumeServer) error {
	_ = "STUB: not implemented"
	return nil
}

// grpcTransport wraps a stream.
type grpcTransport struct {
	mu           sync.RWMutex
	stream       clientproto.CentrifugeUni_ConsumeServer
	closed       bool
	closeCh      chan struct{}
	ack          chan struct{}
	streamDataCh chan []byte
}

func newGRPCTransport(stream clientproto.CentrifugeUni_ConsumeServer, streamDataCh chan []byte, ack chan struct{}) *grpcTransport {
	_ = "STUB: not implemented"
	return nil
}

func (t *grpcTransport) Name() string { _ = "STUB: not implemented"; return "" }

func (t *grpcTransport) AcceptProtocol() string { _ = "STUB: not implemented"; return "" }

func (t *grpcTransport) Protocol() centrifuge.ProtocolType {
	_ = "STUB: not implemented"
	return *new(centrifuge.ProtocolType)
}

func (t *grpcTransport) ProtocolVersion() centrifuge.ProtocolVersion {
	_ = "STUB: not implemented"
	return *new(centrifuge.ProtocolVersion)
}

// Unidirectional returns whether transport is unidirectional.
func (t *grpcTransport) Unidirectional() bool {
	_ = "STUB: not implemented"

	// Emulation ...
	return false
}

func (t *grpcTransport) Emulation() bool {
	_ = "STUB: not implemented"

	// DisabledPushFlags ...
	return false
}

func (t *grpcTransport) DisabledPushFlags() uint64 {
	_ = "STUB: not implemented"

	// PingPongConfig ...
	return 0
}

func (t *grpcTransport) PingPongConfig() centrifuge.PingPongConfig {
	_ = "STUB: not implemented"
	return *new(centrifuge.PingPongConfig)
}

func (t *grpcTransport) Write(message []byte) error { _ = "STUB: not implemented"; return nil }

func (t *grpcTransport) WriteMany(messages ...[]byte) error { _ = "STUB: not implemented"; return nil }

func (t *grpcTransport) Close(_ centrifuge.Disconnect) error { _ = "STUB: not implemented"; return nil }

func handleLog(e centrifuge.LogEntry) { _ = "STUB: not implemented"; return }

var exampleChannel = "unidirectional"

func main() {
	flag.Parse()

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	if *redis {
		redisShardConfigs := []centrifuge.RedisShardConfig{
			{Address: "localhost:6379"},
		}
		var redisShards []*centrifuge.RedisShard
		for _, redisConf := range redisShardConfigs {
			redisShard, err := centrifuge.NewRedisShard(node, redisConf)
			if err != nil {
				log.Fatal(err)
			}
			redisShards = append(redisShards, redisShard)
		}
		// Using Redis Broker here to scale nodes.
		broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
			Shards: redisShards,
		})
		if err != nil {
			log.Fatal(err)
		}
		node.SetBroker(broker)

		presenceManager, err := centrifuge.NewRedisPresenceManager(node, centrifuge.RedisPresenceManagerConfig{
			Shards: redisShards,
		})
		if err != nil {
			log.Fatal(err)
		}
		node.SetPresenceManager(presenceManager)
	}

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Subscriptions: map[string]centrifuge.SubscribeOptions{
				exampleChannel: {},
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})
		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
		transport := client.Transport()
		log.Printf("user %s connected via %s", client.UserID(), transport.Name())
	})

	// Publish to a channel periodically.
	go func() {
		for {
			currentTime := strconv.FormatInt(time.Now().Unix(), 10)
			_, err := node.Publish(exampleChannel, []byte(`{"server_time": "`+currentTime+`"}`))
			if err != nil {
				log.Println(err.Error())
			}
			time.Sleep(5 * time.Second)
		}
	}()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(grpcAuthInterceptor),
		grpc.CustomCodec(&rawCodec{}),
	)
	err := RegisterGRPCServerClient(node, grpcServer, GRPCClientServiceConfig{})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		log.Println("starting GRPC server on :" + strconv.Itoa(*grpcPort))
		listener, err := net.Listen("tcp", ":"+strconv.Itoa(*grpcPort))
		if err != nil {
			log.Fatal(err)
		}
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Serve GRPC: %v", err)
		}
	}()

	go func() {
		if err := http.ListenAndServe(":"+strconv.Itoa(*httpPort), nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node, grpcServer)
	log.Println("bye!")
}

type rawFrame []byte

type rawCodec struct{}

func (c *rawCodec) Marshal(v any) ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (c *rawCodec) Unmarshal(data []byte, v any) error { _ = "STUB: not implemented"; return nil }

func (c *rawCodec) String() string { _ = "STUB: not implemented"; return "" }
