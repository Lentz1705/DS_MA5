package client

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	leaderPort int32
	id         int32
}

func main() {
	n := flag.Int("port", -1, "Connect to server port")
	flag.Parse()

	if *n == -1 {
		flag.Usage()
		os.Exit(2)
	}

	conn, err := grpc.NewClient(fmt.Sprintf(":%d", n),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	idResp, err := pb.NewChatServiceClient(conn).GetId(ctx, &pb.RequestId{Time: getClock(0)})
	myId = idResp.ClientId
	if err != nil {
		log.Fatalf("dial: %v", err)
	}

	conResp, err := pb.NewChatServiceClient(conn).Connect(ctx, &postConnection)
	stream = conResp
}