package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
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

const (
	surveyInternalError  uint32 = 1
	surveyMethodNotFound uint32 = 2
)

func surveyChannels(node *centrifuge.Node) (map[string]int, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func respondChannelsSurvey(node *centrifuge.Node) centrifuge.SurveyReply {
	_ = "STUB: not implemented"
	return *new(centrifuge.SurveyReply)
}

func main() {
	flag.Parse()

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	node.OnSurvey(func(event centrifuge.SurveyEvent, cb centrifuge.SurveyCallback) {
		switch event.Op {
		case "channels":
			cb(respondChannelsSurvey(node))
		default:
			cb(centrifuge.SurveyReply{Code: surveyMethodNotFound})
		}
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(event centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
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

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			time.Sleep(time.Second)
			channels, err := surveyChannels(node)
			if err != nil {
				log.Printf("error channels survey: %v", err)
				continue
			}
			log.Printf("Channels on all nodes: %v", channels)
		}
	}()

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
