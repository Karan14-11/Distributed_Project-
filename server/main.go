package main

import (
	// "context"
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
	// "strconv"
	// "strings"
	// "sync"
	// "time"
	pb "github.com/Karan14-11/Distributed_Project-/proto"
	"google.golang.org/grpc"
)

type Leaderserver struct {
	pb.UnimplementedLeaderNoderserver
	leader_node_port int
	node_port_list   []int
}

type Node struct {
	port int
}

// starting a network
func starting_node(port int) {

	// starting node gets leader +1 port number
	node_port := port + 1

	lis, err = net.Listen("tcp", ":"+strconv.Itoa(node_port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s = grpc.NewServer()
	pb.RegisterNodeServer(s, &Node{port: node_port})
	log.Printf("Starting node on port %d\n", node_port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	// stating leader server

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterLeaderNoderserver(s, &Leaderserver{leader_node_port: port, node_port_list: []int{node_port}})
	log.Printf("Starting node on port %d\n", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

// connecting to an existing network
func connecting_node(port int) {
	// connect to the leader node
	conn, err := grpc.Dial("localhost:"+strconv.Itoa(port), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to leader node: %v", err)
	}
	defer conn.Close()

	client := pb.NewLeaderNoderserverClient(conn)

	// get the list of nodes from the leader node
	response, err := client.GetServerPort(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatalf("Failed to get server: %v", err)
	}
	// log.Printf("Connected to leader node on port %d\n", port)
	server_port_number := response.Port
	log.Printf("Connected to leader node on port %d\n", server_port_number)
}

func main() {

	// IF FIRST NODE IT START THE NETWORK ELSE WILL NEED TO CONNECT TO AN ALREADY EXISTING NETWORK
	first_node := flag.Bool("first_node", false, "Is this the first node?")
	network_port := flag.Int("network_port", 0, "Port to connect to the network")
	// network_ip := flag.String("network_ip", "localhost", "IP address of the network")

	flag.Parse()
	if *first_node && *network_port != 0 {
		log.Println("Starting first node...")
		starting_node(*network_port)
	} else if *network_port != 0 {
		log.Println("Connecting to existing network...")
		connecting_node(*network_port)
	}

	log.Fatalf("You need to either start a network or connect to an already existing network")
}
