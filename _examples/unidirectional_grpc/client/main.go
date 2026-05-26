package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/centrifugal/centrifuge/_examples/unidirectional_grpc/clientproto"

	"google.golang.org/grpc"
)

var (
	serverAddr = flag.String("server_addr", "localhost:10000", "The server address in the format of host:port")
)

func handlePush(push *clientproto.Push) { _ = "STUB: not implemented"; return }

func handleStream(stream clientproto.CentrifugeUni_ConsumeClient) {
	_ = "STUB: not implemented"
	return
}

func main() {
	flag.Parse()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client := clientproto.NewCentrifugeUniClient(conn)

	numFailureAttempts := 0
	for {
		time.Sleep(time.Duration(numFailureAttempts) * time.Second)
		log.Println("establishing a unidirectional stream")
		stream, err := client.Consume(context.Background(), &clientproto.ConnectRequest{})
		if err != nil {
			log.Printf("error establishing stream: %v", err)
			numFailureAttempts++
			continue
		}
		log.Println("stream established")
		numFailureAttempts = 0
		handleStream(stream)
	}
}
