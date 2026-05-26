package main

import (
	"context"
	"log"
	"net/http"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func handleLog(e centrifuge.LogEntry) { _ = "STUB: not implemented"; return }

func authMiddleware(h http.Handler) http.Handler {
	_ = "STUB: not implemented"
	return *new(http.Handler)
}

func handleIndex(w http.ResponseWriter, r *http.Request) { _ = "STUB: not implemented"; return }

func waitExitSignal(n *centrifuge.Node) { _ = "STUB: not implemented"; return }

func main() {
	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Data: []byte(`{}`),
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)

			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnableRecovery:  true,
					EmitPresence:    true,
					EmitJoinLeave:   true,
					PushJoinLeave:   true,
					AllowTagsFilter: true, // Enable tags filtering.
					RecoveryMode:    centrifuge.RecoveryModeCache,
				},
			}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
			result, err := node.Publish(
				e.Channel, e.Data,
				centrifuge.WithHistory(300, time.Minute),
				centrifuge.WithClientInfo(e.ClientInfo),
			)
			cb(centrifuge.PublishReply{
				Result: &result,
			}, err)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", handleIndex)

	// Start publishing ticker data
	go publishTickerData(node)

	log.Print("Starting server, visit http://localhost:8000")
	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

func roundToTwoDecimals(f float64) float64 { _ = "STUB: not implemented"; return 0 }

func publishTickerData(node *centrifuge.Node) { _ = "STUB: not implemented"; return }

// Generate random bid/ask prices.

// Maybe be excluded BTW since sent in tags.
