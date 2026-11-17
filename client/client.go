package client

import (
	"bufio"
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
	conn       *grpc.ClientConn
}

func newClient() *Client {
	return &Client{id: -1}
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

	resp, err := pb.NewCommunicationServiceClient(conn).Connect(ctx, &pb.ConnectReq{Id: c.id})
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	c.conn, _ = grpc.NewClient(fmt.Sprintf(":%d", resp.LeaderPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	c.id = resp.Id
	c.leaderPort = resp.LeaderPort

}

func (c *Client) reconnect() {
	startPort := 50001
	endPort := startPort + 500
	var conn *grpc.ClientConn
	var err error
	for i := startPort; i < endPort; i++ {
		conn, err = grpc.NewClient(fmt.Sprintf(":%d"),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
	}
	ctx := context.Background()
	resp, err := pb.NewCommunicationServiceClient(conn).Connect(ctx, &pb.ConnectReq{Id: c.id})
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	c.conn, _ = grpc.NewClient(fmt.Sprintf(":%d", resp.LeaderPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	c.leaderPort = resp.LeaderPort
}

func loop() {
	var scanner = bufio.NewScanner(os.Stdin)
	for {
		eof := scanner.Scan()
		err := scanner.Err()
		if !eof || err != nil {
			return
		}
		switch scanner.Text() {
		case "bid":

		case "result":

		case "exit":

		default:

		}
	}
}
