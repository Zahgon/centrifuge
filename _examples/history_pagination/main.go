package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
)

type clientMessage struct {
	Timestamp int64  `json:"timestamp"`
	Input     string `json:"input"`
}

func handleLog(e centrifuge.LogEntry) { _ = "STUB: not implemented"; return }

func authMiddleware(h http.Handler) http.Handler {
	_ = "STUB: not implemented"
	return *new(http.Handler)
}

func waitExitSignal(n *centrifuge.Node) { _ = "STUB: not implemented"; return }

const (
	exampleChannel = "chat:index"
	historySize    = 3000
	historyTTL     = 5 * time.Minute
)

// Check whether channel is allowed for subscribing. In real case permission
// check will probably be more complex than in this example.
func channelSubscribeAllowed(channel string) bool { _ = "STUB: not implemented"; return false }

func main() {
	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	broker, _ := centrifuge.NewMemoryBroker(node, centrifuge.MemoryBrokerConfig{})
	node.SetBroker(broker)

	node.OnConnect(func(client *centrifuge.Client) {
		go func() {
			time.Sleep(time.Second)
			client.Disconnect(centrifuge.DisconnectForceReconnect)
		}()

		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
			if !channelSubscribeAllowed(e.Channel) {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnablePositioning: true,
				},
			}, nil)
		})

		client.OnHistory(func(e centrifuge.HistoryEvent, cb centrifuge.HistoryCallback) {
			if !client.IsSubscribed(e.Channel) {
				cb(centrifuge.HistoryReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.HistoryReply{}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
			if !client.IsSubscribed(e.Channel) {
				cb(centrifuge.PublishReply{}, centrifuge.ErrorPermissionDenied)
				return
			}

			var msg clientMessage
			err := json.Unmarshal(e.Data, &msg)
			if err != nil {
				cb(centrifuge.PublishReply{}, centrifuge.ErrorBadRequest)
				return
			}
			msg.Timestamp = time.Now().Unix()
			data, _ := json.Marshal(msg)

			result, err := node.Publish(
				e.Channel, data,
				centrifuge.WithHistory(historySize, historyTTL),
				centrifuge.WithClientInfo(e.ClientInfo),
			)

			cb(centrifuge.PublishReply{Result: &result}, err)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	go func() {
		// Publish to channel periodically.
		i := 1
		for {
			time.Sleep(200 * time.Millisecond)
			_, err := node.Publish(
				"chat:index",
				[]byte(`{"input": "Publish from server `+strconv.Itoa(i)+`"}`),
				centrifuge.WithHistory(historySize, historyTTL),
			)
			if err != nil {
				log.Printf("error publishing to personal channel: %s", err)
			}
			i++
		}
	}()

	websocketHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
	})
	http.Handle("/connection/websocket", authMiddleware(websocketHandler))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
