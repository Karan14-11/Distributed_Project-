package main

import (
	"context"
	"fmt"
	"log"
	// // "maps"
	// "bufio"
	// "io"
	// "io/ioutil"
	"flag"
	"net"
	"os"
	// "os/exec"
	// "path/filepath"
	"strconv"
	// "strings"
	pb "github.com/Karan14-11/Distributed_Project-/proto"
	"google.golang.org/grpc"
	"math/rand"
	"os/exec"
	"sync"
	"time"
)

type Task struct {
	ID        int
	task_type int
	Priority  int
	query     string
}
type Leaderserver struct {
	pb.UnimplementedLeaderNodeServer
}

type SchedulerServer struct {
	pb.UnimplementedSchedulerServer
}

type Node struct {
	pb.UnimplementedServerNodeServer
	port                     int
	heartbeat_resp           sync.Mutex
	node_port_list           []int
	node_type                string
	election_timeout         time.Duration
	election_reset_time      time.Time
	current_leader_port      int
	current_leader_port_node int
	term                     int32
	client_port              int
}

var Leader struct {
	leader_node_port int
	node_port_list   []int
	client_nord_port int
	global_lock      sync.Mutex
	task_queue       []Task
	task_queue_lock  sync.Mutex
	taskid_number    int
}

func getUniqueTaskId() int {
	Leader.task_queue_lock.Lock()
	defer Leader.task_queue_lock.Unlock()

	Leader.taskid_number++
	return Leader.taskid_number

}

func (s *Node) Heartbeat(ctx context.Context, in *pb.Empty) (*pb.Empty, error) {
	s.heartbeat_resp.Lock()
	defer s.heartbeat_resp.Unlock()
	s.election_reset_time = time.Now()
	s.node_type = "follower"

	return &pb.Empty{}, nil
}

// starting a network
func starting_node(port int, client_port int, port_node int, node_list []int) {

	// starting node gets leader +1 port number
	node_port := port_node
	go func() {

		lis, err := net.Listen("tcp", ":"+strconv.Itoa(node_port))
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		pb.RegisterServerNodeServer(s, &Node{port: node_port, node_type: "leader"})
		log.Printf("Starting Node leader on port %d\n", node_port)
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
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()
	s1 := grpc.NewServer()

	Leader.global_lock.Lock()
	Leader.leader_node_port = node_port

	Leader.node_port_list = node_list
	Leader.client_nord_port = client_port
	Leader.task_queue = []Task{}
	Leader.taskid_number = 0

	Leader.global_lock.Unlock()

	go func() {
		for {
			time.Sleep(300 * time.Millisecond) // Wait before sending the next heartbeat

			// Leader.global_lock.Lock()
			nodesPort := make([]int32, len(Leader.node_port_list))
			for i, port := range Leader.node_port_list {
				nodesPort[i] = int32(port)
			}
			// Leader.global_lock.Unlock()

			for _, nodePort := range nodesPort {

				if int(nodePort) == Leader.leader_node_port {
					continue
				}

				conn, err := grpc.Dial("localhost:"+strconv.Itoa(int(nodePort)), grpc.WithInsecure())
				if err != nil {
					log.Printf("Failed to connect to node on port %d for heartbeat: %v", nodePort, err)
					continue
				}

				client := pb.NewServerNodeClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				_, err = client.Heartbeat(ctx, &pb.Empty{})
				if err != nil {
					log.Printf("Failed to send heartbeat to node on port %d: %v", nodePort, err)
				} else {
					// log.Printf("Heartbeat sent to node on port %d", nodePort)
				}

				conn.Close()
			}
		}
	}()

	pb.RegisterLeaderNodeServer(s1, &Leaderserver{})
	log.Printf("Starting Leader node on port %d\n", port)
	if err := s1.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	log.Printf("Starting node on port %d\n", port)

}

func (s *SchedulerServer) Task_Query(ctx context.Context, in *pb.Task_Query) (*pb.Task_Reply, error) {
	// log.Printf("Received Task_Query: %v", in)
	Leader.task_queue_lock.Lock()
	defer Leader.task_queue_lock.Unlock()

	task := Task{
		ID:        getUniqueTaskId(),
		task_type: int(in.TaskType),
		Priority:  0,
		query:     in.DataQuery,
	}
	Leader.task_queue = append(Leader.task_queue, task)

	log.Printf("Task added to queue: %v", task)
	return &pb.Task_Reply{TaskId: int32(task.ID)}, nil

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

func (s *Leaderserver) Heartbeat(ctx context.Context, in *pb.Empty) (*pb.NodeList, error) {

	// Convert s.node_port_list from []int to []int32
	nodesPort := make([]int32, len(Leader.node_port_list))
	for i, port := range Leader.node_port_list {
		nodesPort[i] = int32(port)
	}
	return &pb.NodeList{NodesPort: nodesPort, LeaderPort: int32(Leader.leader_node_port), ClientPort: int32(Leader.client_nord_port), TaskId: int32(Leader.taskid_number)}, nil

}
func (s *Node) RequestVote(ctx context.Context, in *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	if s.term == in.Term-1 && s.node_type == "follower" {
		s.heartbeat_resp.Lock()
		s.term += 1
		s.election_reset_time = time.Now()
		s.heartbeat_resp.Unlock()
		return &pb.RequestVoteReply{Term: s.term, VoteGranted: true}, nil
	} else {
		return &pb.RequestVoteReply{Term: s.term, VoteGranted: false}, nil
	}
}
func new_leader(client_port int, port int, port_node int, node_list []int) {
	scriptPath := "kill_ports.sh"
	_ = os.Chmod(scriptPath, 0755)
	log.Printf("Running script: %s", scriptPath)
	log.Printf("Client port: %d", client_port)
	log.Printf("Port: %d", port)
	_ = exec.Command("./kill_ports.sh", fmt.Sprint(client_port), fmt.Sprint(port)).Run()
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
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()
	s1 := grpc.NewServer()

	// Removed unnecessary nil check for Leader as it is a struct and cannot be nil

	Leader.global_lock.Lock()

	Leader.leader_node_port = port_node
	Leader.node_port_list = node_list
	Leader.client_nord_port = client_port

	Leader.global_lock.Unlock()

	go func() {
		for {
			time.Sleep(300 * time.Millisecond) // Wait before sending the next heartbeat

			// Leader.global_lock.Lock()
			nodesPort := make([]int32, len(Leader.node_port_list))
			for i, port := range Leader.node_port_list {
				nodesPort[i] = int32(port)
			}
			// Leader.global_lock.Unlock()

			for _, nodePort := range nodesPort {

				if int(nodePort) == Leader.leader_node_port {
					continue
				}

				conn, err := grpc.Dial("localhost:"+strconv.Itoa(int(nodePort)), grpc.WithInsecure())
				if err != nil {
					log.Printf("Failed to connect to node on port %d for heartbeat: %v", nodePort, err)
					continue
				}

				client := pb.NewServerNodeClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()

				_, err = client.Heartbeat(ctx, &pb.Empty{})
				if err != nil {
					log.Printf("Failed to send heartbeat to node on port %d: %v", nodePort, err)
				} else {
					// log.Printf("Heartbeat sent to node on port %d", nodePort)
				}

				conn.Close()
			}
		}
	}()

	pb.RegisterLeaderNodeServer(s1, &Leaderserver{})
	log.Printf("Starting node on port %d\n", port)
	if err := s1.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	log.Printf("Starting node on port %d\n", port)

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
	rand.Seed(int64(server_port_number)) // rand.Seed requires int64

	node := &Node{
		port:                int(server_port_number), // Assuming server_port_number is int
		node_type:           "follower",              // Avoid using "type" as a field name
		election_timeout:    time.Duration(rand.Intn(4000)+1500) * time.Millisecond,
		election_reset_time: time.Now(),
		current_leader_port: port,
	}
	go func() {
		for node.current_leader_port_node != port {
			time.Sleep(10 * time.Millisecond)
			// Check if the node has not received a heartbeat for a while

			if time.Now().After(node.election_reset_time.Add(node.election_timeout)) {

				// Start the election process
				log.Printf("Starting election process on port %d\n", server_port_number)
				node.node_type = "candidate"
				node.heartbeat_resp.Lock()
				node.term += 1
				// Create a list of nodes for the election
				votes := 1 // Start with one vote for self
				for _, nodePort := range node.node_port_list {
					if nodePort == node.port {
						continue
					}
					conn, err := grpc.Dial("localhost:"+strconv.Itoa(nodePort), grpc.WithInsecure())
					if err != nil {
						log.Printf("Failed to connect to node on port %d for election: %v", nodePort, err)
						continue
					}

					client := pb.NewServerNodeClient(conn)
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()

					request := &pb.RequestVoteArgs{
						Term:        node.term,
						CandidateId: int32(node.port),
					}

					reply, err := client.RequestVote(ctx, request)
					log.Printf("Reply from node on port %d: %v", nodePort, reply)
					if err != nil {
						log.Printf("Failed to request vote from node on port %d: %v", nodePort, err)
					} else if reply.VoteGranted {
						votes++
					}
					conn.Close()
				}

				// Check if the node has won the election
				log.Printf("Votes received: %d", votes)
				log.Printf("Total nodes: %d", len(node.node_port_list))

				if votes >= (len(node.node_port_list))/2+1 {
					log.Printf("Node on port %d has won the election", node.port)
					node.node_type = "leader"
					newNodePortList := []int{}
					for _, p := range node.node_port_list {
						if p != node.current_leader_port_node {
							newNodePortList = append(newNodePortList, p)
						}
					}
					log.Printf("%v", newNodePortList)
					node.node_port_list = newNodePortList
					node.current_leader_port_node = node.port
					// Remove the port from the node.node_port_list
					node.heartbeat_resp.Unlock()
					new_leader(node.client_port, port, node.current_leader_port, node.node_port_list)
				} else {
					log.Printf("Node on port %d failed to win the election", node.port)
					node.node_type = "candidate"
					node.heartbeat_resp.Unlock()
				}

			}
		}
	}()

	go func() {
		for {
			if node.current_leader_port_node == node.port {
				log.Printf("I am the leader on port %d\n", node.port)
				break
			}
			// Create a connection to the leader node

			conn, err := grpc.Dial("localhost:"+strconv.Itoa(port), grpc.WithInsecure())
			if err != nil {
				log.Printf("Failed to connect to leader node for heartbeat: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			client := pb.NewLeaderNodeClient(conn)

			// Send heartbeat to the leader
			ctx, cancel := context.WithTimeout(context.Background(), node.election_timeout)
			defer cancel()

			resp, err := client.Heartbeat(ctx, &pb.Empty{})
			if err != nil {
				log.Printf("Failed to send heartbeat to leader: %v", err)
				conn.Close()
				time.Sleep(5 * time.Second) // Wait before retrying
				continue
			} else {
				log.Printf("Heartbeat sent to leader on port %d", port)
			}

			// Process the response from the leader
			node.heartbeat_resp.Lock()
			node.node_port_list = make([]int, len(resp.NodesPort))
			for i, port := range resp.NodesPort {
				node.node_port_list[i] = int(port)
			}
			node.current_leader_port_node = int(resp.LeaderPort)

			node.client_port = int(resp.ClientPort)
			node.heartbeat_resp.Unlock()

			conn.Close()
			time.Sleep(5 * time.Second) // Wait before sending the next heartbeat
		}
	}()
	pb.RegisterServerNodeServer(s, node)
	log.Printf("Starting node on port %d\n", server_port_number)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	log.Printf("Starting node on port %d\n", server_port_number)
}

func main() {

	first_node := flag.Bool("first_node", false, "Is this the first node?")
	network_port := flag.Int("network_port", 0, "Port to connect to the network")
	client_port := flag.Int("client_port", 0, "Port to connect to the client")
	// network_ip := flag.String("network_ip", "localhost", "IP address of the network")
	flag.Parse()
	if *first_node && *network_port != 0 && *client_port != 0 {
		log.Println("Starting first node...")
		starting_node(*network_port, *client_port, *network_port+1, []int{*network_port + 1})
	} else if *network_port != 0 {
		log.Println("Connecting to existing network...")
		connecting_node(*network_port)
	}

	log.Fatalf("You need to either start a network or connect to an already existing network")
}

// load balancing (M)
// worker queue (K)
// worker failure (H)
// task workaround in new leader (H)
// logging
// priority in task definition and task dependency(ids of task that are dependent on this task) (K)
// if fails , decrease the priority and reQ (K)
// function that gives port of leader (k)
// client (H)
