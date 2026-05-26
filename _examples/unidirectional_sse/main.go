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
		subs := map[string]centrifuge.SubscribeOptions{
			exampleChannel: {
				EnableRecovery:    true,
				EnablePositioning: true,
			},
		}
		for _, ch := range e.Channels {
			if ch == "test1" || ch == "test2" {
				subs[ch] = centrifuge.SubscribeOptions{}
			}
		}
		return centrifuge.ConnectReply{
			Subscriptions: subs,
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

	mux := http.DefaultServeMux
	mux.Handle("/connection/eventsource", authMiddleware(handleEventsource(node)))
	mux.Handle("/subscribe", handleSubscribe(node))
	mux.Handle("/unsubscribe", handleUnsubscribe(node))
	mux.Handle("/", http.FileServer(http.Dir("./")))

	server := &http.Server{
		Handler:      mux,
		Addr:         ":" + strconv.Itoa(*port),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

func handleEventsource(node *centrifuge.Node) http.HandlerFunc {
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

type eventsourceTransport struct {
	mu           sync.Mutex
	req          *http.Request
	ack          chan struct{}
	messages     chan []byte
	disconnectCh chan *centrifuge.Disconnect
	closedCh     chan struct{}
	closed       bool
}

func newEventsourceTransport(req *http.Request, ack chan struct{}) *eventsourceTransport {
	_ = "STUB: not implemented"
	return nil
}

func (t *eventsourceTransport) Name() string { _ = "STUB: not implemented"; return "" }

func (t *eventsourceTransport) AcceptProtocol() string { _ = "STUB: not implemented"; return "" }

func (t *eventsourceTransport) Protocol() centrifuge.ProtocolType {
	_ = "STUB: not implemented"
	return *new(centrifuge.ProtocolType)
}

func (t *eventsourceTransport) ProtocolVersion() centrifuge.ProtocolVersion {
	_ = "STUB: not implemented"
	return *new(centrifuge.ProtocolVersion)
}

// Unidirectional returns whether transport is unidirectional.
func (t *eventsourceTransport) Unidirectional() bool {
	_ = "STUB: not implemented"

	// DisabledPushFlags ...
	return false
}

func (t *eventsourceTransport) DisabledPushFlags() uint64 {
	_ = "STUB: not implemented"

	// Emulation ...
	return 0
}

func (t *eventsourceTransport) Emulation() bool {
	_ = "STUB: not implemented"

	// PingPongConfig ...
	return false
}

func (t *eventsourceTransport) PingPongConfig() centrifuge.PingPongConfig {
	_ = "STUB: not implemented"
	return *new(centrifuge.PingPongConfig)
}

func (t *eventsourceTransport) Write(message []byte) error { _ = "STUB: not implemented"; return nil }

func (t *eventsourceTransport) WriteMany(messages ...[]byte) error {
	_ = "STUB: not implemented"
	return nil
}

func (t *eventsourceTransport) Close(_ centrifuge.Disconnect) error {
	_ = "STUB: not implemented"
	return nil
}
