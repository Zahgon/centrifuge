package main

import (
	"context"
	"log"
	"net/http"
	"sync"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Counter is a document we sync here. Returned when loading full state.
type Counter struct {
	Version int `json:"version"`
	Value   int `json:"value"`
}

// CounterUpdate represents real-time message we send to the channel.
// Note that we send increments here, not a counter value, to make sure the document is
// properly synchronized when we send changes, not a full state. For the counter we could
// send just an actual value to the channel – we do not make it here intentionally to
// demonstrate the proper data sync.
type CounterUpdate struct {
	Version   int `json:"version"`
	Increment int `json:"increment"`
}

var (
	counter     Counter
	counterLock sync.RWMutex
)

func handleLog(e centrifuge.LogEntry) { _ = "STUB: not implemented"; return }

const exampleChannel = "counter"

// Check whether channel is allowed for subscribing. In real case permission
// check will probably be more complex than in this example.
func channelSubscribeAllowed(channel string) bool { _ = "STUB: not implemented"; return false }

func authMiddleware(h http.Handler) http.Handler {
	_ = "STUB: not implemented"
	return *new(http.Handler)
}

func waitExitSignal(n *centrifuge.Node) { _ = "STUB: not implemented"; return }

func main() {
	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)

			if !channelSubscribeAllowed(e.Channel) {
				log.Printf("user %s disallowed to subscribe on %s", client.UserID(), e.Channel)
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}

			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnableRecovery: true,
				},
			}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.HandleFunc("/api/counter", getCounterHandler)
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/", http.FileServer(http.Dir("./")))

	counter = Counter{Version: 0, Value: 0}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the counter increment simulation.
	go simulateCounterIncrease(ctx, node)

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

func getCounterHandler(w http.ResponseWriter, _ *http.Request) {
	_ = "STUB: not implemented"
	// Emulate delay to ensure data is still synchronized properly and only necessary updates are handled.
	return
}

func simulateCounterIncrease(ctx context.Context, node *centrifuge.Node) {
	_ = "STUB: not implemented"
	return
}

// Publishing under the lock here which is generally not good, but we want
// to emulate transactional outbox or CDC guarantees.

// Emulate transaction rollback.

func publishToChannel(node *centrifuge.Node, version int, increment int) error {
	_ = "STUB: not implemented"
	return nil
}
