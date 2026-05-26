package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
)

var (
	port  = flag.Int("port", 8000, "Port to bind app to")
	redis = flag.Bool("redis", false, "Use Redis")
)

func handleLog(e centrifuge.LogEntry) { _ = "STUB: not implemented"; return }

func authMiddleware(h http.Handler) http.Handler {
	_ = "STUB: not implemented"
	return *new(http.Handler)
}

func waitExitSignal(n *centrifuge.Node) { _ = "STUB: not implemented"; return }

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

	websocketHandler := NewWebsocketHandler(node, WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
	})
	http.Handle("/connection/websocket", authMiddleware(websocketHandler))
	http.Handle("/subscribe", handleSubscribe(node))
	http.Handle("/unsubscribe", handleUnsubscribe(node))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":"+strconv.Itoa(*port), nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

func handleSubscribe(node *centrifuge.Node) http.HandlerFunc {
	_ = "STUB: not implemented"
	return *new(http.HandlerFunc)
}

func handleUnsubscribe(node *centrifuge.Node) http.HandlerFunc {
	_ = "STUB: not implemented"
	return *new(http.HandlerFunc)
}

// websocketTransport is a wrapper struct over websocket connection to fit session
// interface so client will accept it.
type websocketTransport struct {
	mu        sync.RWMutex
	writeMu   sync.Mutex // sync general write with unidirectional ping write.
	conn      *websocket.Conn
	closed    bool
	closeCh   chan struct{}
	graceCh   chan struct{}
	opts      websocketTransportOptions
	pingTimer *time.Timer
}

type websocketTransportOptions struct {
	protoType          centrifuge.ProtocolType
	pingInterval       time.Duration
	writeTimeout       time.Duration
	compressionMinSize int
}

func newWebsocketTransport(conn *websocket.Conn, opts websocketTransportOptions, graceCh chan struct{}) *websocketTransport {
	_ = "STUB: not implemented"
	return nil
}

func (t *websocketTransport) ping() { _ = "STUB: not implemented"; return }

func (t *websocketTransport) addPing() { _ = "STUB: not implemented"; return }

// Name returns name of transport.
func (t *websocketTransport) Name() string { _ = "STUB: not implemented"; return "" }

func (t *websocketTransport) AcceptProtocol() string {
	_ = "STUB: not implemented"

	// Protocol returns transport protocol.
	return ""
}

func (t *websocketTransport) Protocol() centrifuge.ProtocolType {
	_ = "STUB: not implemented"
	return *

	// ProtocolVersion returns transport ProtocolVersion.
	new(centrifuge.ProtocolType)
}

func (t *websocketTransport) ProtocolVersion() centrifuge.ProtocolVersion {
	_ = "STUB: not implemented"
	return *new(centrifuge.ProtocolVersion)
}

// Unidirectional returns whether transport is unidirectional.
func (t *websocketTransport) Unidirectional() bool {
	_ = "STUB: not implemented"

	// Emulation ...
	return false
}

func (t *websocketTransport) Emulation() bool {
	_ = "STUB: not implemented"

	// DisabledPushFlags ...
	return false
}

func (t *websocketTransport) DisabledPushFlags() uint64 {
	_ = "STUB: not implemented"

	// PingPongConfig ...
	return 0
}

func (t *websocketTransport) PingPongConfig() centrifuge.PingPongConfig {
	_ = "STUB: not implemented"
	return *new(centrifuge.PingPongConfig)
}

func (t *websocketTransport) writeData(data []byte) error { _ = "STUB: not implemented"; return nil }

func (t *websocketTransport) Write(message []byte) error { _ = "STUB: not implemented"; return nil }

func (t *websocketTransport) WriteMany(messages ...[]byte) error {
	_ = "STUB: not implemented"
	return nil
}

const closeFrameWait = 5 * time.Second

// Close closes transport.
func (t *websocketTransport) Close(_ centrifuge.Disconnect) error {
	_ = "STUB: not implemented"
	return nil
}

// Defaults.
const (
	DefaultWebsocketPingInterval     = 25 * time.Second
	DefaultWebsocketWriteTimeout     = 1 * time.Second
	DefaultWebsocketMessageSizeLimit = 65536 // 64KB
)

// WebsocketConfig represents config for WebsocketHandler.
type WebsocketConfig struct {
	// CompressionLevel sets a level for websocket compression.
	// See possible value description at https://golang.org/pkg/compress/flate/#NewWriter
	CompressionLevel int

	// CompressionMinSize allows setting minimal limit in bytes for
	// message to use compression when writing it into client connection.
	// By default, it's 0 - i.e. all messages will be compressed when
	// WebsocketCompression enabled and compression negotiated with client.
	CompressionMinSize int

	// ReadBufferSize is a parameter that is used for raw websocket Upgrader.
	// If set to zero reasonable default value will be used.
	ReadBufferSize int

	// WriteBufferSize is a parameter that is used for raw websocket Upgrader.
	// If set to zero reasonable default value will be used.
	WriteBufferSize int

	// MessageSizeLimit sets the maximum size in bytes of allowed message from client.
	// By default DefaultWebsocketMaxMessageSize will be used.
	MessageSizeLimit int

	// CheckOrigin func to provide custom origin check logic.
	// nil means allow all origins.
	CheckOrigin func(r *http.Request) bool

	// PingInterval sets interval server will send ping messages to clients.
	// By default DefaultPingInterval will be used.
	PingInterval time.Duration

	// WriteTimeout is maximum time of write message operation.
	// Slow client will be disconnected.
	// By default DefaultWebsocketWriteTimeout will be used.
	WriteTimeout time.Duration

	// Compression allows to enable websocket permessage-deflate
	// compression support for raw websocket connections. It does
	// not guarantee that compression will be used - i.e. it only
	// says that server will try to negotiate it with client.
	Compression bool

	// UseWriteBufferPool enables using buffer pool for writes.
	UseWriteBufferPool bool
}

// WebsocketHandler handles WebSocket client connections. WebSocket protocol
// is a bidirectional connection between a client an a server for low-latency
// communication.
type WebsocketHandler struct {
	node    *centrifuge.Node
	upgrade *websocket.Upgrader
	config  WebsocketConfig
}

var writeBufferPool = &sync.Pool{}

// NewWebsocketHandler creates new WebsocketHandler.
func NewWebsocketHandler(n *centrifuge.Node, c WebsocketConfig) *WebsocketHandler {
	_ = "STUB: not implemented"
	return nil
}

type ConnectRequest struct {
	Token   string                       `json:"token,omitempty"`
	Data    json.RawMessage              `json:"data,omitempty"`
	Subs    map[string]*SubscribeRequest `json:"subs,omitempty"`
	Name    string                       `json:"name,omitempty"`
	Version string                       `json:"version,omitempty"`
}

type SubscribeRequest struct {
	Recover bool   `json:"recover,omitempty"`
	Epoch   string `json:"epoch,omitempty"`
	Offset  uint64 `json:"offset,omitempty"`
}

func (s *WebsocketHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	_ = "STUB: not implemented"
	return
}

// Separate goroutine for better GC of caller's data.

// https://github.com/gorilla/websocket/issues/448

func sameHostOriginCheck() func(r *http.Request) bool { _ = "STUB: not implemented"; return nil }

func checkSameHost(r *http.Request) error { _ = "STUB: not implemented"; return nil }
