package main

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge/_examples/compression_playground/apppb"

	"github.com/centrifugal/centrifuge"
)

func simulateMatch(ctx context.Context, num int32, node *centrifuge.Node, useProtobufPayload bool) {
	_ = "STUB: not implemented"
	// Predefined lists of player names for each team.
	return
}

// Example setup

// Total time for the simulation in seconds
// Total number of events to simulate
// Time between events

// Sleep between events

// Calculate minute based on event occurrence.

// Choose one of the 11 players randomly

func chooseRandomEventType(r *rand.Rand) apppb.EventType {
	_ = "STUB: not implemented"
	return *new(apppb.EventType)
}

func chooseRandomTeam(r *rand.Rand, match *apppb.Match) *apppb.Team {
	_ = "STUB: not implemented"
	return nil
}

// Helper function to create players with names from a given list
func assignNamesToPlayers(names []string) []*apppb.Player { _ = "STUB: not implemented"; return nil }

func auth(h http.Handler) http.Handler { _ = "STUB: not implemented"; return *new(http.Handler) }

// Put authentication Credentials into request Context.
// Since we don't have any session backend here we simply
// set user ID as empty string. Users with empty ID called
// anonymous users, in real app you should decide whether
// anonymous users allowed to connect to your server or not.

// This is a hack for the playground.

func main() {
	// Node is the core object in Centrifuge library responsible for
	// many useful things. For example Node allows publishing messages
	// into channels with its Publish method. Here we initialize Node
	// with Config which has reasonable defaults for zero values.
	node, err := centrifuge.New(centrifuge.Config{
		LogLevel: centrifuge.LogLevelDebug,
		LogHandler: func(entry centrifuge.LogEntry) {
			log.Println(entry.Message, entry.Fields)
		},
		//GetChannelMediumOptions: func(channel string) (centrifuge.ChannelMediumOptions, bool) {
		//	return centrifuge.ChannelMediumOptions{
		//		KeepLatestPublication: true,
		//		EnableQueue:           true,
		//		BroadcastDelay:        500 * time.Millisecond,
		//	}, true
		//},
	})
	if err != nil {
		log.Fatal(err)
	}

	//redisShardConfigs := []centrifuge.RedisShardConfig{
	//	{Address: "localhost:6379"},
	//}
	//var redisShards []*centrifuge.RedisShard
	//for _, redisConf := range redisShardConfigs {
	//	redisShard, err := centrifuge.NewRedisShard(node, redisConf)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	redisShards = append(redisShards, redisShard)
	//}
	//
	//broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
	//	// And configure a couple of shards to use.
	//	Shards: redisShards,
	//})
	//if err != nil {
	//	log.Fatal(err)
	//}
	//node.SetBroker(broker)

	node.OnConnecting(func(ctx context.Context, event centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		cred, _ := centrifuge.GetCredentials(ctx)
		reply := centrifuge.ConnectReply{}
		if strings.Contains(string(cred.Info), "delay") {
			reply.MaxMessagesInFrame = -1
			reply.WriteDelay = 200 * time.Millisecond
			reply.ReplyWithoutQueue = true
		}
		return reply, nil
	})

	// Set ConnectHandler called when client successfully connected to Node.
	// Your code inside a handler must be synchronized since it will be called
	// concurrently from different goroutines (belonging to different client
	// connections). See information about connection life cycle in library readme.
	// This handler should not block – so do minimal work here, set required
	// connection event handlers and return.
	node.OnConnect(func(client *centrifuge.Client) {
		// In our example transport will always be Websocket but it can be different.
		transportName := client.Transport().Name()
		// In our example clients connect with JSON protocol but it can also be Protobuf.
		transportProto := client.Transport().Protocol()
		log.Printf("client connected via %s (%s)", transportName, transportProto)

		var useProtobufPayload bool
		if strings.Contains(string(client.Info()), "protobuf") {
			useProtobufPayload = true
		}

		go func() {
			log.Printf("using protobuf payload: %v", useProtobufPayload)
			simulateMatch(client.Context(), 0, node, useProtobufPayload)
		}()

		// Set SubscribeHandler to react on every channel subscription attempt
		// initiated by a client. Here you can theoretically return an error or
		// disconnect a client from a server if needed. But here we just accept
		// all subscriptions to all channels. In real life you may use a more
		// complex permission check here. The reason why we use callback style
		// inside client event handlers is that it gives a possibility to control
		// operation concurrency to developer and still control order of events.
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("client subscribes on channel %s", e.Channel)
			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnableRecovery:    true,
					RecoveryMode:      centrifuge.RecoveryModeCache,
					AllowedDeltaTypes: []centrifuge.DeltaType{centrifuge.DeltaTypeFossil},
				},
			}, nil)
		})

		// By default, clients can not publish messages into channels. By setting
		// PublishHandler we tell Centrifuge that publish from a client-side is
		// possible. Now each time client calls publish method this handler will be
		// called and you have a possibility to validate publication request. After
		// returning from this handler Publication will be published to a channel and
		// reach active subscribers with at most once delivery guarantee. In our simple
		// chat app we allow everyone to publish into any channel but in real case
		// you may have more validation.
		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("client publishes into channel %s: %s", e.Channel, string(e.Data))
			cb(centrifuge.PublishReply{}, nil)
		})

		// Set Disconnect handler to react on client disconnect events.
		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("client disconnected: %d (%s)", e.Code, e.Reason)
		})
	})

	// Run node. This method does not block. See also node.Shutdown method
	// to finish application gracefully.
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	// Now configure HTTP routes.

	http.Handle("/connection/websocket/no_compression", auth(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))

	http.Handle("/connection/websocket/with_compression", auth(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		Compression:        true,
		CompressionMinSize: 1,
		CompressionLevel:   1,
	})))

	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/json", serveJsonApp)
	http.HandleFunc("/protobuf", serveProtobufApp)

	// Serve static files from the /static folder
	fs := http.FileServer(http.Dir("static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))

	log.Printf("Starting server, visit http://localhost:8000")
	if err := http.ListenAndServe("127.0.0.1:8000", nil); err != nil {
		log.Fatal(err)
	}
}

func serveIndex(w http.ResponseWriter, r *http.Request) { _ = "STUB: not implemented"; return }

func serveJsonApp(w http.ResponseWriter, r *http.Request) { _ = "STUB: not implemented"; return }

func serveProtobufApp(w http.ResponseWriter, r *http.Request) { _ = "STUB: not implemented"; return }
