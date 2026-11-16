package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "homework5/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedCommunicationServiceServer
	leaderPort  int32
	id          int32
	port        int32
	replicaCons []int32
}

func main() {
	id := flag.Int("id", -1, "id for node")
	totalNodes := flag.Int("totalNodes", -1, "totalNodes in server")
	flag.Parse()

	if *id == -1 || *totalNodes == -1 {
		flag.Usage()
		os.Exit(2)
	}

	port := 50000 + *id
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()

	n := newNode(*id, *totalNodes)
	go n.process()

	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func newNode(id int, totalNodes int) Node {
	node := Node{}
	node.port = 50000 + int32(id)
	for i := 1; i <= totalNodes; i++ {
		port := 50000 + int32(i)
		if node.port == port {
			continue
		}
		node.replicaCons = append(node.replicaCons, port)
	}
	return node
}

func (n *Node) process() {
	for {
		time.Sleep(500 * time.Millisecond)
		go n.election()
	}
}

func (n *Node) election() {
	count := 0
	totalNodes := len(n.replicaCons)
	for i := totalNodes - 1; i > int(n.id); i-- {
		conn, err := grpc.NewClient(fmt.Sprintf("%d", n.replicaCons[i]), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("dial: %v", err)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		Resp, err := pb.NewCommunicationServiceClient(conn).Elect(ctx, &pb.ElectReq{})
		if err == nil {
			n.leaderPort = Resp.LeaderPort
			cancel()
			continue
		}

		count++
		cancel()
	}
	if totalNodes-count == int(n.id) {
		n.leaderPort = n.port
	}
}

func (n *Node) Elect(ctx context.Context, msg *pb.ElectReq) (*pb.ElectAck, error) {
	return &pb.ElectAck{
		LeaderPort: n.leaderPort,
	}, nil
}

func (n *Node) isLeader() bool {
	return n.leaderPort == n.port
}
