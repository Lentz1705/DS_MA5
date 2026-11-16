package client

import (
	"context"
	"flag"
	"fmt"
	pb "homework5/grpc"
	"log"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	leaderPort int32
	id         int32
}

func newClient() *Client {
	return &Client{}
}

func main() {
	port := flag.Int("port", -1, "Connect to server port")
	flag.Parse()

	if *port == -1 {
		flag.Usage()
		os.Exit(2)
	}

	c := newClient()

	conn, err := grpc.NewClient(fmt.Sprintf(":%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	idResp, err := pb.NewCommunicationServiceClient(conn).Connect(ctx, &pb.ConnectReq{})
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	c.id = idResp.Id
	c.leaderPort = idResp.LeaderPort

}	