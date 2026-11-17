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
	leaderPort    int32
	leaderId      int32
	id            int32
	port          int32
	replicaPorts  map[int32]int32
	cIdCount      int32
	winnerId      int32
	highestBid    int32
	activeAuction bool
	totalNodes    int32
	replicaConns  map[int32]*grpc.ClientConn
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

	time.Sleep(time.Second * 1)

	go n.election()

	time.Sleep(500 * time.Millisecond)
	go n.auction()

	log.Printf("Node %d listening on %d", n.id, port)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func (n *Node) auction() {
	n.activeAuction = true

	time.Sleep(60 * time.Second)

	n.activeAuction = false
}

func (n *Node) createConnection(id int32) {
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", n.replicaPorts[id]), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Couldn't create connection to port: %d", n.replicaPorts[id])
		return
	}
	n.replicaConns[id] = conn
}

func newNode(id int, totalNodes int) *Node {
	node := Node{}
	node.port = 50000 + int32(id)
	node.cIdCount = 0
	node.id = int32(id)
	node.totalNodes = int32(totalNodes)
	node.leaderId = int32(totalNodes)
	node.leaderPort = 50000 + node.leaderId
	node.replicaPorts = make(map[int32]int32)
	node.replicaConns = make(map[int32]*grpc.ClientConn)
	for i := 1; i <= totalNodes; i++ {
		id := int32(i)
		port := 50000 + id
		if node.id == id {
			continue
		}
		node.replicaPorts[id] = port
		node.createConnection(id)
	}
	return &node
}

func (n *Node) election() {
	for {
		time.Sleep(1000 * time.Millisecond)
		lp := n.leaderPort
		higherAlive := false
		for i := n.totalNodes; i > n.id; i-- {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			Resp, err := pb.NewCommunicationServiceClient(n.replicaConns[int32(i)]).Elect(ctx, &pb.ElectReq{})
			if err == nil {
				higherAlive = true
				n.leaderPort = Resp.LeaderPort
				n.leaderId = int32(i)
				cancel()
				break
			} else {
				cancel()
			}
		}
		if !higherAlive {
			n.leaderPort = n.port
		}
		if lp != n.leaderPort {
			log.Printf("%d Leader changed from %d to %d", n.id, lp, n.leaderPort)
		}
	}
}

func (n *Node) Elect(ctx context.Context, msg *pb.ElectReq) (*pb.ElectAck, error) {
	return &pb.ElectAck{
		LeaderPort: n.leaderPort,
	}, nil
}

func (n *Node) Connect(ctx context.Context, msg *pb.ConnectReq) (*pb.ConnectAck, error) {
	if n.leaderPort == n.port {
		if msg.Id == -1 {
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
				Id:         n.cIdCount,
				LeaderPort: n.leaderPort,
			}, nil

		} else {
			return &pb.ConnectAck{
				Id:         msg.Id,
				LeaderPort: n.leaderPort,
			}, nil
		}
	}
	conn := n.replicaConns[n.leaderId]

	return pb.NewCommunicationServiceClient(conn).Connect(ctx, msg)
}

func (n *Node) Bid(ctx context.Context, msg *pb.BidReq) (*pb.BidAck, error) {
	if !n.activeAuction {
		return &pb.BidAck{
			Result: &pb.BidAck_Exception{
				Exception: &pb.Exception{
					Reason: "The auction is currently not active",
				},
			},
		}, nil
	}
	if n.highestBid < msg.Amount {
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
	if n.activeAuction {
		return &pb.ResultAck{
			Result: &pb.ResultAck_HighestBid{
				HighestBid: n.highestBid,
			},
		}, nil
	} else {
		return &pb.ResultAck{
			Result: &pb.ResultAck_WinnerId{
				WinnerId: n.winnerId,
			},
		}, nil
	}
}

func (n *Node) isLeader() bool {
	return n.leaderPort == n.port
}

func (n *Node) SyncReplicas(ctx context.Context, msg *pb.Update) (*pb.UpdateAck, error) {
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
	for _, conn := range n.replicaConns {
		ctx := context.Background()
		pb.NewCommunicationServiceClient(conn).SyncReplicas(ctx, msg)
	}
}
