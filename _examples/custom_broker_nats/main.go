package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifuge/_examples/custom_broker_nats/natsbroker"
)

var (
	port = flag.Int("port", 8000, "Port to bind app to")
)

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

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EmitPresence:  true,
					EmitJoinLeave: true,
					PushJoinLeave: true,
				},
			}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
			cb(centrifuge.PublishReply{}, nil)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
	})

	broker, err := natsbroker.New(node, natsbroker.Config{
		Prefix: "centrifuge-nats-broker-example",
	})
	if err != nil {
		log.Fatal(err)
	}

	node.SetBroker(broker)

	// Let RedisPresenceManager do the presence stuff.
	redisShard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{
		Address: "localhost:6379",
	})
	if err != nil {
		log.Fatal(err)
	}
	presenceManager, err := centrifuge.NewRedisPresenceManager(node, centrifuge.RedisPresenceManagerConfig{
		Shards: []*centrifuge.RedisShard{redisShard},
	})
	if err != nil {
		log.Fatal(err)
	}
	node.SetPresenceManager(presenceManager)

	// If you don't need presence then you can go without Redis.
	// Just remove RedisPresenceManager initialization above and uncomment
	// the following line of code:
	// node.SetPresenceManager(nil)

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
