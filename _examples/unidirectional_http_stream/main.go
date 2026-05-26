package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
)

var (
	port     = flag.Int("port", 8000, "Port to bind app to")
	redis    = flag.Bool("redis", false, "Use Redis")
	tls      = flag.Bool("tls", false, "Use TLS")
	keyFile  = flag.String("key_file", "server.key", "path to TLS key file")
	certFile = flag.String("cert_file", "server.crt", "path to TLS crt file")
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
				exampleChannel: {
					EnableRecovery:    true,
					EnablePositioning: true,
					Data:              []byte(`{"message": "welcome to a channel"}`),
				},
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
			_, err := node.Publish(exampleChannel, []byte(`{"server_time": "`+currentTime+`"}`), centrifuge.WithHistory(10, time.Minute))
			if err != nil {
				log.Println(err.Error())
			}
			time.Sleep(5 * time.Second)
		}
	}()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/stream", authMiddleware(handleStream(node)))
	http.Handle("/subscribe", handleSubscribe(node))
	http.Handle("/unsubscribe", handleUnsubscribe(node))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if *tls {
			//if *useHttp3 {
			//	if err := http3.ListenAndServe("0.0.0.0:443", *certFile, *keyFile, nil); err != nil {
			//		log.Fatal(err)
			//	}
			//} else {
			if err := http.ListenAndServeTLS(":"+strconv.Itoa(*port), *certFile, *keyFile, nil); err != nil {
				log.Fatal(err)
			}
			//}
		} else {
			if err := http.ListenAndServe(":"+strconv.Itoa(*port), nil); err != nil {
				log.Fatal(err)
			}
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

func handleStream(node *centrifuge.Node) http.HandlerFunc {
	_ = "STUB: not implemented"
	return *new(http.HandlerFunc)
}

// need to execute this after client closeFn.

func handleSubscribe(node *centrifuge.Node) http.HandlerFunc {
	_ = "STUB: not implemented"
	return *new(http.HandlerFunc)
}

func handleUnsubscribe(node *centrifuge.Node) http.HandlerFunc {
	_ = "STUB: not implemented"
	return *new(http.HandlerFunc)
}

type streamTransport struct {
	mu           sync.Mutex
	req          *http.Request
	ack          chan struct{}
	messages     chan []byte
	disconnectCh chan *centrifuge.Disconnect
	closedCh     chan struct{}
	closed       bool
}

func newStreamTransport(req *http.Request, ack chan struct{}) *streamTransport {
	_ = "STUB: not implemented"
	return nil
}

func (t *streamTransport) Name() string { _ = "STUB: not implemented"; return "" }

func (t *streamTransport) AcceptProtocol() string { _ = "STUB: not implemented"; return "" }

func (t *streamTransport) Protocol() centrifuge.ProtocolType {
	_ = "STUB: not implemented"
	return *new(centrifuge.ProtocolType)
}

// ProtocolVersion ...
func (t *streamTransport) ProtocolVersion() centrifuge.ProtocolVersion {
	_ = "STUB: not implemented"
	return *new(centrifuge.ProtocolVersion)
}

// Unidirectional returns whether transport is unidirectional.
func (t *streamTransport) Unidirectional() bool {
	_ = "STUB: not implemented"

	// Emulation ...
	return false
}

func (t *streamTransport) Emulation() bool {
	_ = "STUB: not implemented"

	// DisabledPushFlags ...
	return false
}

func (t *streamTransport) DisabledPushFlags() uint64 {
	_ = "STUB: not implemented"

	// PingPongConfig ...
	return 0
}

func (t *streamTransport) PingPongConfig() centrifuge.PingPongConfig {
	_ = "STUB: not implemented"
	return *new(centrifuge.PingPongConfig)
}

func (t *streamTransport) Write(message []byte) error { _ = "STUB: not implemented"; return nil }

func (t *streamTransport) WriteMany(messages ...[]byte) error {
	_ = "STUB: not implemented"
	return nil
}

func (t *streamTransport) Close(_ centrifuge.Disconnect) error {
	_ = "STUB: not implemented"
	return nil
}
