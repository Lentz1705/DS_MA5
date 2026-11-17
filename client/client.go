package main

import (
	"bufio"
	"context"
	"fmt"
	pb "homework5/grpc"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

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
	c := newClient()

	c.connect()
	log.Printf("connected")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go c.loop(&wg)
	wg.Wait()
}

func (c *Client) connect() {
	startPort := 50001
	endPort := startPort + 500
	var conn *grpc.ClientConn
	var err error
	for i := startPort; i < endPort; i++ {
		conn, err = grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", i),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			log.Printf("We have a connection")
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
	c.id = resp.Id
}

func (c *Client) loop(wg *sync.WaitGroup) {
	defer wg.Done()
	var scanner = bufio.NewScanner(os.Stdin)
	for {
		eof := scanner.Scan()
		err := scanner.Err()
		if !eof || err != nil {
			return
		}
		args := strings.Split(scanner.Text(), " ")
		ctx := context.Background()
		switch args[0] {
		case "bid":
			bidAmount, err := strconv.Atoi(args[1])
			if err != nil {
				break
			}
			resp, err := pb.NewCommunicationServiceClient(c.conn).Bid(ctx, &pb.BidReq{Amount: int32(bidAmount), Id: c.id})
			if err != nil {
				c.connect()
				log.Println("Server unavailable. Try again")
				break
			}
			switch r := resp.Result.(type) {
			case *pb.BidAck_Success:
				log.Println("Succesful bid. You are now winning")
			case *pb.BidAck_Failure:
				log.Println(r.Failure.Reason)
			case *pb.BidAck_Exception:
				log.Println(r.Exception.Reason)
			}
		case "result":
			resp, err := pb.NewCommunicationServiceClient(c.conn).Result(ctx, &pb.ResultReq{})
			if err != nil {
				c.connect()
				log.Println("Server unavailable. Try again")
				break
			}
			switch r := resp.Result.(type) {
			case *pb.ResultAck_HighestBid:
				log.Printf("The current highest bid is %d", r.HighestBid)
			case *pb.ResultAck_WinnerId:
				if r.WinnerId == c.id {
					log.Printf("You won the auction")
				} else {
					log.Printf("You lost the auction")
				}
			}

		case "exit":
			return

		default:

		}
	}
}
