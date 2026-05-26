package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"time"

	"github.com/centrifugal/centrifuge"
)

var (
	port = flag.Int("port", 8000, "Port to bind app to")
)

const exampleChannel = "blink182"

func handleLog(e centrifuge.LogEntry) { _ = "STUB: not implemented"; return }

func authMiddleware(h http.Handler) http.Handler {
	_ = "STUB: not implemented"
	return *new(http.Handler)
}

func waitExitSignal(n *centrifuge.Node) { _ = "STUB: not implemented"; return }

func main() {
	flag.Parse()

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Subscriptions: map[string]centrifuge.SubscribeOptions{
				exampleChannel: {},
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			go func() {
				if !client.IsSubscribed("blink182") {
					// Prevent multiple resubscribe on many quick clicks on a screen in
					// this example. In real case you most probably don't need this check
					// since subscribe command should be issued once by your backend logic.
					return
				}
				_ = node.Unsubscribe("42", exampleChannel)
				time.Sleep(time.Second)
				_ = node.Subscribe("42", exampleChannel)
			}()
			cb(centrifuge.RPCReply{}, nil)
		})
	})

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

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":"+strconv.Itoa(*port), nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
