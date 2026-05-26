package main

import (
	"context"
	"log"
	"net/http"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
)

func handleLog(e centrifuge.LogEntry) { _ = "STUB: not implemented"; return }

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

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		cred, _ := centrifuge.GetCredentials(ctx)
		userID := cred.UserID

		presenceStats, err := node.PresenceStats("#" + userID)
		if err != nil {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectServerError
		}
		if presenceStats.NumClients >= 1 {
			err = node.Disconnect(
				userID,
				centrifuge.WithCustomDisconnect(centrifuge.DisconnectConnectionLimit),
				centrifuge.WithDisconnectClientWhitelist([]string{e.ClientID}),
			)
			if err != nil {
				return centrifuge.ConnectReply{}, centrifuge.DisconnectServerError
			}
		}

		return centrifuge.ConnectReply{
			// Subscribe to personal several server-side channel.
			Subscriptions: map[string]centrifuge.SubscribeOptions{
				"#" + userID: {EmitPresence: true},
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	websocketHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
	})
	http.Handle("/connection/websocket", authMiddleware(websocketHandler))

	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		log.Println("starting http server, visit http://localhost:8000")
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
