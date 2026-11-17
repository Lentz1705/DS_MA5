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
	cIdCount    int32
	winnerId    int32
	highestBid  int32
	
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
	pb.RegisterCommunicationServiceServer(srv, n)

	go n.process()

	log.Printf("Node %d listening on %s", id, port)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func newNode(id int, totalNodes int) *Node {
	node := Node{}
	node.port = 50000 + int32(id)
	node.cIdCount = 0
	for i := 1; i <= totalNodes; i++ {
		port := 50000 + int32(i)
		if node.port == port {
			continue
		}
		node.replicaCons = append(node.replicaCons, port)
	}
	return &node
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

func (n *Node) Connect(ctx context.Context, msg *pb.ConnectReq) (*pb.ConnectAck, error) {
    if n.leaderPort == n.port {
		n.cIdCount++
		update := &pb.Update{
			Update: &pb.Update_SyncClientReq{
				SyncClientReq: &pb.SyncClientCountReq{
					CIdCount: n.cIdCount,
				},
			},
		}
		n.synchronize(update)
        return &pb.ConnectAck{
			Id:		 n.cIdCount,
            LeaderPort: n.leaderPort,
        }, nil
    }
    conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", n.leaderPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, err
    }
    defer conn.Close()
    return pb.NewCommunicationServiceClient(conn).Connect(ctx, msg)
}

func (n *Node) Bid(ctx context.Context, msg *pb.BidReq) (*pb.BidAck, error) {
	if (n.highestBid < msg.Amount) {
		n.highestBid = msg.Amount
		n.winnerId = msg.Id
		update := &pb.Update{
			Update: &pb.Update_SyncAuctionreq{
				SyncAuctionreq: &pb.SyncAuctionReq{
					WinnerId:   n.winnerId,
					HighestBid: n.highestBid,
				},
			},
		}
		n.synchronize(update)
		return &pb.BidAck{Result: &pb.BidAck_Success{}}, nil
	} else {
		return &pb.BidAck{
			Result: &pb.BidAck_Failure{
				Failure: &pb.Failure{
					Reason: "Bid amount is not higher than current highest bid",
				},
			},
		}, nil
	}
}

func (n *Node) Result(ctx context.Context, msg *pb.ResultReq) (*pb.ResultAck, error) {
	panic("implement me")
}

func (n *Node) isLeader() bool {
	return n.leaderPort == n.port
}

func (n *Node) syncReplicas(ctx context.Context, msg *pb.Update) (*pb.UpdateAck, error) {
	switch r := msg.Update.(type) {
	case *pb.Update_SyncAuctionreq:
		n.winnerId = r.SyncAuctionreq.WinnerId
		n.highestBid = r.SyncAuctionreq.HighestBid
	case *pb.Update_SyncClientReq:
		n.cIdCount = r.SyncClientReq.CIdCount
	case nil:
	}

	return &pb.UpdateAck{}, nil
}

func (n *Node) synchronize(msg *pb.Update) {
	for i := 0; i < int(n.id); i++ {
		conn, err := grpc.NewClient(fmt.Sprintf("%d", n.replicaCons[i]), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}

		ctx, cancel := context.WithCancel(context.Background())
		pb.NewCommunicationServiceClient(conn).SyncReplicas(ctx, msg)
		cancel()
	}
}
