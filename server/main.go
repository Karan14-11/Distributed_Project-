// // load balancing (M)
// // worker queue (K)
// // worker failure (H)
// // task workaround in new leader (H)
// // logging
// // priority in task definition and task dependency(ids of task that are dependent on this task) (K)
// // if fails , decrease the priority and reQ (K)
// // function that gives port of leader (k)
// // client (H)
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/Karan14-11/Distributed_Project-/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Task struct {
	ID         int
	TaskType   int
	Priority   int
	Query      string
	AssignedTo int
}

type Leaderserver struct {
	pb.UnimplementedLeaderNodeServer
}

type SchedulerServer struct {
	pb.UnimplementedSchedulerServer
}

type Node struct {
	pb.UnimplementedServerNodeServer
	port              int
	heartbeatMutex    sync.Mutex
	nodePortList      []int
	nodeType          string
	electionTimeout   time.Duration
	electionResetTime time.Time
	currentLeaderPort int
	localLeaderPort   int
	term              int32
	clientPort        int
	cpuUsage          float64
	cpuUsageMutex     sync.Mutex
	initialized       bool
	initMutex         sync.Mutex
	activeTasks       int
	activeTasksMutex  sync.Mutex
}

var Leader struct {
	leaderNodePort  int
	nodePortList    []int
	clientNodePort  int
	globalMutex     sync.Mutex
	taskQueue       []Task
	taskQueueMutex  sync.Mutex
	taskIDNumber    int
	workerLoads     map[int]float64 // Changed to float64 for CPU percentage
	workerLoadMutex sync.Mutex
}

// GetCPUUsage returns the current CPU usage as a percentage (0-100)
func getCPUUsage() float64 {
	// Cross-platform implementation that works on both Linux and macOS
	var cmd *exec.Cmd

	// Try Linux first
	cmd = exec.Command("sh", "-c", "grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage}'")
	output, err := cmd.Output()
	if err == nil && len(output) > 0 {
		usage, err := strconv.ParseFloat(strings.TrimSpace(string(output)), 64)
		if err == nil {
			return usage
		}
	}

	// Fallback to macOS command
	cmd = exec.Command("sh", "-c", "top -l 1 | grep -E '^CPU' | awk '{print $3 + $5}'")
	output, err = cmd.Output()
	if err == nil && len(output) > 0 {
		usage, err := strconv.ParseFloat(strings.TrimSpace(string(output)), 64)
		if err == nil {
			return usage
		}
	}

	// If all else fails, simulate CPU usage to allow system to function
	// This simulates a random CPU usage between 20% and 80%
	log.Printf("Falling back to simulated CPU usage")
	return 20.0 + rand.Float64()*60.0
}

func getUniqueTaskID() int {
	Leader.taskQueueMutex.Lock()
	defer Leader.taskQueueMutex.Unlock()
	Leader.taskIDNumber++
	return Leader.taskIDNumber
}

func (s *Node) Heartbeat(ctx context.Context, in *pb.Empty) (*pb.Empty, error) {
	s.heartbeatMutex.Lock()
	defer s.heartbeatMutex.Unlock()
	s.electionResetTime = time.Now()
	s.nodeType = "follower"
	return &pb.Empty{}, nil
}

func (s *Node) AssignTask(ctx context.Context, in *pb.TaskAssignment) (*pb.TaskAssignmentResponse, error) {
	// Get current CPU load
	cpuLoad := getCPUUsage()

	// Increment active tasks counter
	s.activeTasksMutex.Lock()
	s.activeTasks++
	activeTaskCount := s.activeTasks
	s.activeTasksMutex.Unlock()

	// Calculate effective load based on both CPU and task count
	effectiveLoad := cpuLoad + float64(activeTaskCount*10) // Each task adds 10% to effective load

	s.cpuUsageMutex.Lock()
	s.cpuUsage = effectiveLoad // Use effective load instead of just CPU
	s.cpuUsageMutex.Unlock()

	log.Printf("Worker %d: Starting task %d (CPU: %.2f%%, Tasks: %d)",
		s.port, in.TaskId, cpuLoad, activeTaskCount)
	log.Printf("Worker %d: Effective load for task %d: %.2f%%",
		s.port, in.TaskId, effectiveLoad)

	// Report new load immediately
	conn, err := grpc.Dial("localhost:"+strconv.Itoa(s.currentLeaderPort), grpc.WithInsecure())
	if err == nil {
		client := pb.NewLeaderNodeClient(conn)
		_, _ = client.ReportLoad(context.Background(), &pb.WorkerLoad{
			Port: int32(s.port),
			Load: int32(effectiveLoad),
		})
		conn.Close()
	}

	// Process the task in a goroutine
	go func() {
		// Simulate task processing
		processingTime := time.Duration(rand.Intn(2)+1) * time.Second // Shorter for better testing
		time.Sleep(processingTime)

		// Decrement active tasks counter
		s.activeTasksMutex.Lock()
		s.activeTasks--
		newActiveTaskCount := s.activeTasks
		s.activeTasksMutex.Unlock()

		log.Printf("Worker %d: Completed task %d (remaining tasks: %d)",
			s.port, in.TaskId, newActiveTaskCount)

		// Report updated CPU load after task completion
		currentCpuLoad := getCPUUsage()
		newEffectiveLoad := currentCpuLoad + float64(newActiveTaskCount*10)

		s.cpuUsageMutex.Lock()
		s.cpuUsage = newEffectiveLoad
		s.cpuUsageMutex.Unlock()

		// Report to leader
		conn, err := grpc.Dial("localhost:"+strconv.Itoa(s.currentLeaderPort), grpc.WithInsecure())
		if err != nil {
			log.Printf("Worker %d: Failed to connect to leader: %v", s.port, err)
			return
		}
		defer conn.Close()

		client := pb.NewLeaderNodeClient(conn)
		_, err = client.ReportLoad(context.Background(), &pb.WorkerLoad{
			Port: int32(s.port),
			Load: int32(newEffectiveLoad),
		})

		if err != nil {
			log.Printf("Worker %d: Failed to report load: %v", s.port, err)
		}
	}()

	return &pb.TaskAssignmentResponse{Success: true}, nil
}

func (s *Leaderserver) ReportLoad(ctx context.Context, in *pb.WorkerLoad) (*pb.Empty, error) {
	Leader.workerLoadMutex.Lock()
	defer Leader.workerLoadMutex.Unlock()

	// Store load as float64 for more accurate comparisons
	Leader.workerLoads[int(in.Port)] = float64(in.Load)
	log.Printf("Worker %d reported CPU load: %.2f%%", in.Port, float64(in.Load))

	return &pb.Empty{}, nil
}

// Add a lastSelectedWorker variable to the Leader struct
var lastSelectedWorker int

func getLeastLoadedWorker() (int, bool) {
	Leader.workerLoadMutex.Lock()
	defer Leader.workerLoadMutex.Unlock()

	if len(Leader.workerLoads) == 0 {
		return 0, false
	}

	// First, find the minimum load
	minLoad := float64(100) // Start with maximum possible CPU percentage
	for _, load := range Leader.workerLoads {
		if load < minLoad {
			minLoad = load
		}
	}

	// Find all workers with load close to minimum (within 1%)
	candidateWorkers := []int{}
	for port, load := range Leader.workerLoads {
		if load <= minLoad+1.0 { // Consider workers within 1% of min load as equal
			candidateWorkers = append(candidateWorkers, port)
		}
	}

	// Sort candidate workers for consistent ordering
	for i := 0; i < len(candidateWorkers)-1; i++ {
		for j := i + 1; j < len(candidateWorkers); j++ {
			if candidateWorkers[i] > candidateWorkers[j] {
				candidateWorkers[i], candidateWorkers[j] = candidateWorkers[j], candidateWorkers[i]
			}
		}
	}

	// If we have multiple candidates with similar load, use round-robin
	selectedPort := candidateWorkers[0] // Default to first worker

	if len(candidateWorkers) > 1 {
		// Find index of last selected worker
		lastIndex := -1
		for i, port := range candidateWorkers {
			if port == lastSelectedWorker {
				lastIndex = i
				break
			}
		}

		// Select next worker in the list (round-robin)
		nextIndex := (lastIndex + 1) % len(candidateWorkers)
		selectedPort = candidateWorkers[nextIndex]
	}

	// Update last selected worker
	lastSelectedWorker = selectedPort

	log.Printf("Selected worker %d with CPU load %.2f%% (from %d candidates)",
		selectedPort, Leader.workerLoads[selectedPort], len(candidateWorkers))
	return selectedPort, true
}

func processTaskQueue() {
	for {
		time.Sleep(1 * time.Second)

		Leader.taskQueueMutex.Lock()
		if len(Leader.taskQueue) == 0 {
			Leader.taskQueueMutex.Unlock()
			continue
		}

		log.Printf("Processing %d tasks in queue", len(Leader.taskQueue))

		// Sort tasks by priority
		tasks := make([]Task, len(Leader.taskQueue))
		copy(tasks, Leader.taskQueue)
		Leader.taskQueueMutex.Unlock()

		// Sort by priority (higher priority first)
		for i := 0; i < len(tasks)-1; i++ {
			for j := i + 1; j < len(tasks); j++ {
				if tasks[i].Priority < tasks[j].Priority {
					tasks[i], tasks[j] = tasks[j], tasks[i]
				}
			}
		}

		for _, task := range tasks {
			workerPort, found := getLeastLoadedWorker()
			if !found {
				log.Printf("No available workers for task %d", task.ID)
				task.Priority++
				Leader.taskQueueMutex.Lock()
				Leader.taskQueue = append(Leader.taskQueue, task)
				Leader.taskQueueMutex.Unlock()
				continue
			}

			log.Printf("Assigning task %d to worker %d", task.ID, workerPort)

			conn, err := grpc.Dial("localhost:"+strconv.Itoa(workerPort), grpc.WithInsecure())
			if err != nil {
				log.Printf("Failed to connect to worker %d: %v", workerPort, err)
				task.Priority++
				Leader.taskQueueMutex.Lock()
				Leader.taskQueue = append(Leader.taskQueue, task)
				Leader.taskQueueMutex.Unlock()
				continue
			}

			client := pb.NewServerNodeClient(conn)
			_, err = client.AssignTask(context.Background(), &pb.TaskAssignment{
				TaskId:   int32(task.ID),
				TaskType: int32(task.TaskType),
				Query:    task.Query,
				Priority: int32(task.Priority),
			})
			conn.Close()

			if err != nil {
				log.Printf("Failed to assign task %d: %v", task.ID, err)
				task.Priority++
				Leader.taskQueueMutex.Lock()
				Leader.taskQueue = append(Leader.taskQueue, task)
				Leader.taskQueueMutex.Unlock()
				continue
			}

			// Remove assigned task
			Leader.taskQueueMutex.Lock()
			for i, t := range Leader.taskQueue {
				if t.ID == task.ID {
					Leader.taskQueue = append(Leader.taskQueue[:i], Leader.taskQueue[i+1:]...)
					break
				}
			}
			Leader.taskQueueMutex.Unlock()
		}
	}
}

func startingNode(port int, clientPort int, nodePort int, initialNodes []int) {
	// Start node server
	go func() {
		lis, err := net.Listen("tcp", ":"+strconv.Itoa(nodePort))
		if err != nil {
			log.Fatalf("Failed to listen on node port %d: %v", nodePort, err)
		}
		s := grpc.NewServer()
		node := &Node{
			port:              nodePort,
			nodeType:          "follower",
			electionTimeout:   time.Duration(rand.Intn(4000)+1500) * time.Millisecond,
			electionResetTime: time.Now(),
			currentLeaderPort: port,
			cpuUsage:          0,
		}
		pb.RegisterServerNodeServer(s, node)
		reflection.Register(s)
		log.Printf("Node server started on port %d", nodePort)

		// Start CPU monitoring in background
		go func() {
			for {
				time.Sleep(2 * time.Second)
				cpuLoad := getCPUUsage()

				node.cpuUsageMutex.Lock()
				node.cpuUsage = cpuLoad
				node.cpuUsageMutex.Unlock()

				// Report to leader if we're not the leader
				if node.nodeType != "leader" {
					conn, err := grpc.Dial("localhost:"+strconv.Itoa(node.currentLeaderPort), grpc.WithInsecure())
					if err != nil {
						log.Printf("Failed to connect to leader for load reporting: %v", err)
						continue
					}

					client := pb.NewLeaderNodeClient(conn)
					_, err = client.ReportLoad(context.Background(), &pb.WorkerLoad{
						Port: int32(node.port),
						Load: int32(cpuLoad),
					})

					if err != nil {
						log.Printf("Failed to report load: %v", err)
					}

					conn.Close()
				}
			}
		}()

		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve node: %v", err)
		}
	}()

	// Start client server
	go func() {
		lis, err := net.Listen("tcp", ":"+strconv.Itoa(clientPort))
		if err != nil {
			log.Fatalf("Failed to listen on client port %d: %v", clientPort, err)
		}
		s := grpc.NewServer()
		pb.RegisterSchedulerServer(s, &SchedulerServer{})
		reflection.Register(s)
		log.Printf("Client server started on port %d", clientPort)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve client: %v", err)
		}
	}()

	// Initialize leader state
	Leader.globalMutex.Lock()
	Leader.leaderNodePort = nodePort
	Leader.nodePortList = initialNodes
	Leader.clientNodePort = clientPort
	Leader.taskQueue = []Task{}
	Leader.taskIDNumber = 0
	Leader.workerLoads = make(map[int]float64)
	lastSelectedWorker = 0 // Initialize the round-robin counter
	Leader.globalMutex.Unlock()

	// Start task processor
	go processTaskQueue()

	// Start heartbeat sender
	go func() {
		for {
			time.Sleep(300 * time.Millisecond)

			Leader.globalMutex.Lock()
			nodePorts := make([]int, len(Leader.nodePortList))
			copy(nodePorts, Leader.nodePortList)
			Leader.globalMutex.Unlock()

			for _, port := range nodePorts {
				if port == Leader.leaderNodePort {
					continue
				}

				conn, err := grpc.Dial("localhost:"+strconv.Itoa(port), grpc.WithInsecure())
				if err != nil {
					log.Printf("Failed to connect to node %d: %v", port, err)
					Leader.workerLoadMutex.Lock()
					delete(Leader.workerLoads, port)
					Leader.workerLoadMutex.Unlock()
					continue
				}

				client := pb.NewServerNodeClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err = client.Heartbeat(ctx, &pb.Empty{})
				cancel()
				conn.Close()

				if err != nil {
					log.Printf("Failed to send heartbeat to node %d: %v", port, err)
					Leader.workerLoadMutex.Lock()
					delete(Leader.workerLoads, port)
					Leader.workerLoadMutex.Unlock()
				}
			}
		}
	}()

	// Start leader server
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		log.Fatalf("Failed to listen on leader port %d: %v", port, err)
	}
	s := grpc.NewServer()
	pb.RegisterLeaderNodeServer(s, &Leaderserver{})
	reflection.Register(s)
	log.Printf("Leader server started on port %d", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve leader: %v", err)
	}
}

func (s *SchedulerServer) QueryTask(ctx context.Context, in *pb.Task_Query) (*pb.Task_Reply, error) {
	task := Task{
		ID:       getUniqueTaskID(),
		TaskType: int(in.TaskType),
		Priority: int(in.Priority),
		Query:    in.DataQuery,
	}

	Leader.taskQueueMutex.Lock()
	Leader.taskQueue = append(Leader.taskQueue, task)
	Leader.taskQueueMutex.Unlock()

	log.Printf("Added task %d to queue (type: %d, priority: %d)", task.ID, task.TaskType, task.Priority)
	return &pb.Task_Reply{TaskId: int32(task.ID)}, nil
}

func (s *Leaderserver) GetServerPort(ctx context.Context, in *pb.Empty) (*pb.ServerPort, error) {
	Leader.globalMutex.Lock()
	defer Leader.globalMutex.Unlock()

	newPort := 50051
	for _, port := range Leader.nodePortList {
		if port >= newPort {
			newPort = port + 1
		}
	}

	Leader.nodePortList = append(Leader.nodePortList, newPort)
	Leader.workerLoadMutex.Lock()
	Leader.workerLoads[newPort] = 0
	Leader.workerLoadMutex.Unlock()

	log.Printf("Assigned new worker port: %d", newPort)
	return &pb.ServerPort{Port: int32(newPort)}, nil
}

func (s *Leaderserver) Heartbeat(ctx context.Context, in *pb.Empty) (*pb.NodeList, error) {
	Leader.globalMutex.Lock()
	defer Leader.globalMutex.Unlock()

	nodePorts := make([]int32, len(Leader.nodePortList))
	for i, port := range Leader.nodePortList {
		nodePorts[i] = int32(port)
	}

	return &pb.NodeList{
		NodesPort:  nodePorts,
		LeaderPort: int32(Leader.leaderNodePort),
		ClientPort: int32(Leader.clientNodePort),
		TaskId:     int32(Leader.taskIDNumber),
	}, nil
}

func (s *Node) RequestVote(ctx context.Context, in *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	s.heartbeatMutex.Lock()
	defer s.heartbeatMutex.Unlock()

	if in.Term > s.term {
		s.term = in.Term
		s.electionResetTime = time.Now()
		log.Printf("Node %d: Voting for candidate %d in term %d", s.port, in.CandidateId, in.Term)
		return &pb.RequestVoteReply{Term: s.term, VoteGranted: true}, nil
	}
	return &pb.RequestVoteReply{Term: s.term, VoteGranted: false}, nil
}

func promoteToLeader(clientPort, networkPort, nodePort int, nodePortList []int) {
	log.Printf("Node %d promoting to leader", nodePort)
	_ = exec.Command("./kill_ports.sh", fmt.Sprint(clientPort), fmt.Sprint(networkPort)).Run()

	// Start client server
	go func() {
		lis, err := net.Listen("tcp", ":"+strconv.Itoa(clientPort))
		if err != nil {
			log.Fatalf("Failed to listen on client port %d: %v", clientPort, err)
		}
		s := grpc.NewServer()
		pb.RegisterSchedulerServer(s, &SchedulerServer{})
		reflection.Register(s)
		log.Printf("Client server started on port %d", clientPort)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to serve client: %v", err)
		}
	}()

	// Initialize leader state
	Leader.globalMutex.Lock()
	Leader.leaderNodePort = nodePort
	Leader.nodePortList = nodePortList
	Leader.clientNodePort = clientPort
	Leader.workerLoads = make(map[int]float64)
	Leader.globalMutex.Unlock()

	// Start task processor
	go processTaskQueue()

	// Start heartbeat sender
	go func() {
		for {
			time.Sleep(300 * time.Millisecond)

			Leader.globalMutex.Lock()
			nodePorts := make([]int, len(Leader.nodePortList))
			copy(nodePorts, Leader.nodePortList)
			Leader.globalMutex.Unlock()

			for _, port := range nodePorts {
				if port == Leader.leaderNodePort {
					continue
				}

				conn, err := grpc.Dial("localhost:"+strconv.Itoa(port), grpc.WithInsecure())
				if err != nil {
					log.Printf("Failed to connect to node %d: %v", port, err)
					Leader.workerLoadMutex.Lock()
					delete(Leader.workerLoads, port)
					Leader.workerLoadMutex.Unlock()
					continue
				}

				client := pb.NewServerNodeClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err = client.Heartbeat(ctx, &pb.Empty{})
				cancel()
				conn.Close()

				if err != nil {
					log.Printf("Failed to send heartbeat to node %d: %v", port, err)
					Leader.workerLoadMutex.Lock()
					delete(Leader.workerLoads, port)
					Leader.workerLoadMutex.Unlock()
				}
			}
		}
	}()

	// Start leader server
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(networkPort))
	if err != nil {
		log.Fatalf("Failed to listen on leader port %d: %v", networkPort, err)
	}
	s := grpc.NewServer()
	pb.RegisterLeaderNodeServer(s, &Leaderserver{})
	reflection.Register(s)
	log.Printf("Leader server started on port %d", networkPort)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve leader: %v", err)
	}
}

func connectToNetwork(networkPort int) {
	// Get a worker port from leader
	conn, err := grpc.Dial("localhost:"+strconv.Itoa(networkPort), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to leader: %v", err)
	}
	defer conn.Close()

	client := pb.NewLeaderNodeClient(conn)
	resp, err := client.GetServerPort(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatalf("Failed to get worker port: %v", err)
	}

	workerPort := int(resp.Port)
	log.Printf("Starting worker on port %d", workerPort)

	// Start worker server
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(workerPort))
	if err != nil {
		log.Fatalf("Failed to listen on worker port %d: %v", workerPort, err)
	}
	s := grpc.NewServer()

	node := &Node{
		port:              workerPort,
		nodeType:          "follower",
		electionTimeout:   time.Duration(rand.Intn(4000)+1500) * time.Millisecond,
		electionResetTime: time.Now(),
		currentLeaderPort: networkPort,
		cpuUsage:          0,
	}

	// Start CPU monitoring
	go func() {
		for {
			time.Sleep(2 * time.Second)
			cpuLoad := getCPUUsage()

			node.cpuUsageMutex.Lock()
			node.cpuUsage = cpuLoad
			node.cpuUsageMutex.Unlock()

			// Only report if we're not in election process
			if node.nodeType == "follower" {
				// Report to leader
				conn, err := grpc.Dial("localhost:"+strconv.Itoa(networkPort), grpc.WithInsecure())
				if err != nil {
					log.Printf("Failed to connect to leader for load reporting: %v", err)
					continue
				}

				client := pb.NewLeaderNodeClient(conn)
				_, err = client.ReportLoad(context.Background(), &pb.WorkerLoad{
					Port: int32(node.port),
					Load: int32(cpuLoad),
				})

				if err != nil {
					log.Printf("Failed to report load: %v", err)
				}

				conn.Close()
			}
		}
	}()

	// Start election handler
	go func() {
		for {
			if node.nodeType == "leader" {
				return
			}

			if time.Since(node.electionResetTime) > node.electionTimeout {
				log.Printf("Node %d: Starting election (term %d)", node.port, node.term+1)
				node.nodeType = "candidate"
				node.heartbeatMutex.Lock()
				node.term++
				node.electionResetTime = time.Now()
				votes := 1
				total_votes:=1
				for _, port := range node.nodePortList {
					if port == node.port {
						continue
					}

					conn, err := grpc.Dial("localhost:"+strconv.Itoa(port), grpc.WithInsecure())
					if err != nil {
						log.Printf("Node %d: Failed to connect to node %d: %v", node.port, port, err)
						continue
					}

					client := pb.NewServerNodeClient(conn)
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					resp, err := client.RequestVote(ctx, &pb.RequestVoteArgs{
						Term:        node.term,
						CandidateId: int32(node.port),
					})
					cancel()
					conn.Close()

					if err != nil {
						log.Printf("Node %d: Failed to request vote from node %d: %v", node.port, port, err)
					} else if resp.VoteGranted {
						votes++
						total_votes++;
						log.Printf("Node %d: Received vote from node %d", node.port, port)
					}else {
						total_votes++
					}
				}

				if votes > total_votes/2 {
					log.Printf("Node %d: Won election with %d votes (term %d)", node.port, votes, node.term)
					node.nodeType = "leader"
					portes:= []int{}
					for _, port := range node.nodePortList {
						if port != node.localLeaderPort {
							portes = append(portes, port)
						}
					}
					node.localLeaderPort= node.port
					node.nodePortList = append(portes, node.port)
					node.heartbeatMutex.Unlock()
					promoteToLeader(node.clientPort, networkPort, node.port, node.nodePortList)
					return
				} else {
					log.Printf("Node %d: Lost election with %d votes", node.port, votes)
					node.nodeType = "follower"
					node.heartbeatMutex.Unlock()
				}
			}

			// Regular heartbeat handling
			conn, err := grpc.Dial("localhost:"+strconv.Itoa(networkPort), grpc.WithInsecure())
			if err != nil {
				log.Printf("Node %d: Failed to connect to leader: %v", node.port, err)
				time.Sleep(5 * time.Second)
				continue
			}

			client := pb.NewLeaderNodeClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := client.Heartbeat(ctx, &pb.Empty{})
			cancel()

			if err != nil {
				log.Printf("Node %d: Failed to get heartbeat: %v", node.port, err)
				conn.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			node.heartbeatMutex.Lock()
			node.nodePortList = make([]int, len(resp.NodesPort))
			for i, port := range resp.NodesPort {
				node.nodePortList[i] = int(port)
			}
			node.clientPort = int(resp.ClientPort)
			node.localLeaderPort = int(resp.LeaderPort)
			node.heartbeatMutex.Unlock()
			conn.Close()

			time.Sleep(5 * time.Second)
		}
	}()

	pb.RegisterServerNodeServer(s, node)
	reflection.Register(s)
	log.Printf("Worker server started on port %d", workerPort)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve worker: %v", err)
	}
}

func main() {
	firstNode := flag.Bool("first_node", false, "Is this the first node?")
	networkPort := flag.Int("network_port", 0, "Port to connect to the network")
	clientPort := flag.Int("client_port", 0, "Port to connect to the client")
	flag.Parse()

	if *firstNode && *networkPort != 0 && *clientPort != 0 {
		log.Println("Starting first node...")
		startingNode(*networkPort, *clientPort, *networkPort+1, []int{*networkPort + 1})
	} else if *networkPort != 0 {
		log.Println("Connecting to existing network...")
		connectToNetwork(*networkPort)
	} else {
		log.Fatal("You need to either start a network or connect to an existing network")
	}
}
