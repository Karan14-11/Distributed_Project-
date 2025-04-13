package main

import (
	"context"
	// "fmt"
	"log"
	// // "maps"
	// "bufio"
	// "io"
	// "io/ioutil"
	"net"
	// "os"
	"flag"
	// "os/exec"
	// "path/filepath"
	"strconv"
	// "strings"
	pb "github.com/Karan14-11/Distributed_Project-/proto"
	"google.golang.org/grpc"
	"sync"
	"time"
)

type Leaderserver struct {
	pb.UnimplementedLeaderNodeServer
}

type SchedulerServer struct {
	pb.UnimplementedSchedulerServer
}

type Node struct {
	pb.UnimplementedServerNodeServer
	port           int
	heartbeat_resp sync.Mutex
	node_port_list []int
}

var Leader struct {
	leader_node_port int
	node_port_list   []int
	client_nord_port int
	global_lock      sync.Mutex
}

// starting a network
func starting_node(port int, client_port int) {

	// starting node gets leader +1 port number
	node_port := port + 1
	go func() {

		lis, err := net.Listen("tcp", ":"+strconv.Itoa(node_port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		pb.RegisterServerNodeServer(s, &Node{port: node_port})
		log.Printf("Starting node on port %d\n", node_port)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// stating leader server
	go func() {

		lis, err := net.Listen("tcp", ":"+strconv.Itoa(client_port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s1 := grpc.NewServer()
		pb.RegisterSchedulerServer(s1, &SchedulerServer{})
		log.Printf("Starting client node on port %d\n", client_port)
		if err := s1.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	defer lis.Close()
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s1 := grpc.NewServer()

	Leader.global_lock.Lock()
	Leader.leader_node_port = port
	Leader.node_port_list = []int{node_port}
	Leader.client_nord_port = client_port
	Leader.global_lock.Unlock()

	pb.RegisterLeaderNodeServer(s1, &Leaderserver{})
	log.Printf("Starting node on port %d\n", port)
	if err := s1.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	log.Printf("Starting node on port %d\n", port)

}

func (s *Leaderserver) GetServerPort(ctx context.Context, in *pb.Empty) (*pb.ServerPort, error) {

	newport := 50051
	for _, port := range Leader.node_port_list {
		newport = max(newport, port)
	}
	newport += 1
	Leader.global_lock.Lock()
	Leader.node_port_list = append(Leader.node_port_list, newport)
	Leader.global_lock.Unlock()
	return &pb.ServerPort{Port: int32(newport)}, nil
}

func (s *Leaderserver) heartbeat(ctx context.Context, in *pb.Empty) (*pb.NodeList, error) {

	// Convert s.node_port_list from []int to []int32
	nodesPort := make([]int32, len(Leader.node_port_list))
	for i, port := range Leader.node_port_list {
		nodesPort[i] = int32(port)
	}
	return &pb.NodeList{NodesPort: nodesPort, LeaderPort: int32(Leader.leader_node_port)}, nil

}

// connecting to an existing network
func connecting_node(port int) {
	// connect to the leader node
	conn, err := grpc.Dial("localhost:"+strconv.Itoa(port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to leader node: %v", err)
	}
	defer conn.Close()

	client := pb.NewLeaderNodeClient(conn)

	// get the list of nodes from the leader node
	response, err := client.GetServerPort(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatalf("Failed to get server: %v", err)
	}
	// log.Printf("Connected to leader node on port %d\n", port)
	server_port_number := response.Port
	log.Printf("running on %d\n", server_port_number)
	// log.Printf("Connected to leader node on port %d\n", server_port_number)
	// start the node server
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(int(server_port_number)))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	node := &Node{port: int(server_port_number)}
	pb.RegisterServerNodeServer(s, node)
	log.Printf("Starting node on port %d\n", server_port_number)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	log.Printf("Starting node on port %d\n", server_port_number)
	go func() {
		for {
			// Create a connection to the leader node
			conn, err := grpc.Dial("localhost:"+strconv.Itoa(port), grpc.WithInsecure())
			if err != nil {
				log.Printf("Failed to connect to leader node for heartbeat: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			client := pb.NewLeaderNodeClient(conn)

			// Send heartbeat to the leader
			resp, err := client.Heartbeat(context.Background(), &pb.Empty{})
			if err != nil {
				log.Printf("Failed to send heartbeat to leader: %v", err)
			} else {
				log.Printf("Heartbeat sent to leader on port %d", port)
			}

			// Process the response from the leader
			node.heartbeat_resp.Lock()
			node.node_port_list = make([]int, len(resp.NodesPort))
			for i, port := range resp.NodesPort {
				node.node_port_list[i] = int(port)
			}
			node.heartbeat_resp.Unlock()

			conn.Close()
			time.Sleep(5 * time.Second) // Wait before sending the next heartbeat
		}
	}()
}

func main() {

	// IF FIRST NODE IT START THE NETWORK ELSE WILL NEED TO CONNECT TO AN ALREADY EXISTING NETWORK
	first_node := flag.Bool("first_node", false, "Is this the first node?")
	network_port := flag.Int("network_port", 0, "Port to connect to the network")
	client_port := flag.Int("client_port", 0, "Port to connect to the client")
	// network_ip := flag.String("network_ip", "localhost", "IP address of the network")

	flag.Parse()
	if *first_node && *network_port != 0 && *client_port != 0 {
		log.Println("Starting first node...")
		starting_node(*network_port, *client_port)
	} else if *network_port != 0 {
		log.Println("Connecting to existing network...")
		connecting_node(*network_port)
	}

	log.Fatalf("You need to either start a network or connect to an already existing network")
}

// TODO
// 3. On the starting the starting node is bydefault the leader. Once it fails the election will reoccur and the port
//50051 will shift to the next leader in the election and he will host there (Mahika )
