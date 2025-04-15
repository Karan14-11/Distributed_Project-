// // load balancing (M) DONE
// // worker queue (K) NOT NEEDED
// // worker failure (H)
// // task workaround in new leader (H)
// // logging DONE
// // priority in task definition and task dependency(ids of task that are dependent on this task) (K) DONE
// // if fails , decrease the priority and reQ (K) DONE
// // function that gives port of leader (k) DONE
// // client (M)
package main

import (
	"context"
	"errors"
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
	ID             int
	TaskType       int
	Priority       int
	Query          string
	AssignedTo     int
	status         bool
	dependencyList []int
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
	timer_worker    map[int]time.Time
	workerLoadMutex sync.Mutex
	taskCompletion  map[int]bool
	taskAssign      map[int][]Task
}

type LeaderData struct {
	leaderNodePort  int
	nodePortList    []int
	clientNodePort  int
	globalMutex     sync.Mutex
	taskQueue       []Task
	taskQueueMutex  sync.Mutex
	taskIDNumber    int
	workerLoads     map[int]float64 // Changed to float64 for CPU percentage
	timer_worker    map[int]time.Time
	workerLoadMutex sync.Mutex
	taskCompletion  map[int]bool
	taskAssign      map[int][]Task
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

// func (s *Node) AssignTask(ctx context.Context, in *pb.TaskAssignment) (*pb.TaskAssignmentResponse, error) {
// 	// Get current CPU load
// 	cpuLoad := getCPUUsage()

// 	// Increment active tasks counter
// 	s.activeTasksMutex.Lock()
// 	s.activeTasks++
// 	activeTaskCount := s.activeTasks
// 	s.activeTasksMutex.Unlock()

// 	// Calculate effective load based on both CPU and task count
// 	effectiveLoad := cpuLoad + float64(activeTaskCount*10) // Each task adds 10% to effective load

// 	s.cpuUsageMutex.Lock()
// 	s.cpuUsage = effectiveLoad // Use effective load instead of just CPU
// 	s.cpuUsageMutex.Unlock()

// 	log.Printf("Worker %d: Starting task %d (CPU: %.2f%%, Tasks: %d)",
// 		s.port, in.TaskId, cpuLoad, activeTaskCount)
// 	log.Printf("Worker %d: Effective load for task %d: %.2f%%",
// 		s.port, in.TaskId, effectiveLoad)

// 	// Report new load immediately
// 	conn, err := grpc.Dial("localhost:"+strconv.Itoa(s.currentLeaderPort), grpc.WithInsecure())
// 	if err == nil {
// 		client := pb.NewLeaderNodeClient(conn)
// 		_, _ = client.ReportLoad(context.Background(), &pb.WorkerLoad{
// 			Port: int32(s.port),
// 			Load: int32(effectiveLoad),
// 		})
// 		conn.Close()
// 	}

// 	// Process the task in a goroutine
// 	go func() {
// 		// Simulate task processing
// 		processingTime := time.Duration(rand.Intn(2)+1) * time.Second // Shorter for better testing
// 		time.Sleep(processingTime)

// 		// Decrement active tasks counter
// 		s.activeTasksMutex.Lock()
// 		s.activeTasks--
// 		newActiveTaskCount := s.activeTasks
// 		s.activeTasksMutex.Unlock()

// 		log.Printf("Worker %d: Completed task %d (remaining tasks: %d)",
// 			s.port, in.TaskId, newActiveTaskCount)

// 		// Notify leader about task completion
// 		conn, err := grpc.Dial("localhost:"+strconv.Itoa(s.currentLeaderPort), grpc.WithInsecure())
// 		if err != nil {
// 			log.Printf("Worker %d: Failed to connect to leader to report task completion: %v", s.port, err)
// 			return
// 		}
// 		defer conn.Close()

// 		client := pb.NewLeaderNodeClient(conn)
// 		_, err = client.TaskCompletionResponse(context.Background(), &pb.Task_Reply{TaskId: in.TaskId})

// 		if err != nil {
// 			log.Printf("Worker %d: Failed to report task completion for task %d: %v", s.port, in.TaskId, err)
// 		} else {
// 			log.Printf("Worker %d: Successfully reported task completion for task %d", s.port, in.TaskId)
// 		}

// 		// Report updated CPU load after task completion
// 		currentCpuLoad := getCPUUsage()
// 		newEffectiveLoad := currentCpuLoad + float64(newActiveTaskCount*10)

// 		s.cpuUsageMutex.Lock()
// 		s.cpuUsage = newEffectiveLoad
// 		s.cpuUsageMutex.Unlock()

// 		// Report to leader
// 		_, err = client.ReportLoad(context.Background(), &pb.WorkerLoad{
// 			Port: int32(s.port),
// 			Load: int32(newEffectiveLoad),
// 		})

// 		if err != nil {
// 			log.Printf("Worker %d: Failed to report load: %v", s.port, err)
// 		}
// 	}()

// 	return &pb.TaskAssignmentResponse{Success: true}, nil
// }

// Updated AssignTask method for the Node struct
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

	// Get task type
	taskType := int(in.TaskType)
	taskTypeStr := taskTypeToString(taskType)

	log.Printf("Worker %d: Starting task %d (%s) with priority %s (CPU: %.2f%%, Tasks: %d)",
		s.port, in.TaskId, taskTypeStr, priorityToString(int(in.Priority)), cpuLoad, activeTaskCount)
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
		// Determine processing time based on task type and complexity
		var processingTime time.Duration
		var result string

		// Execute the task
		if taskType == 0 { // Matrix Multiplication
			// Parse and execute matrix multiplication
			result = executeMatrixMultiplication(in.Query)
			processingTime = calculateProcessingTime(taskType, in.Query)
		} else if taskType == 1 { // Prime Factorization
			// Parse and execute prime factorization
			result = executePrimeFactorization(in.Query)
			processingTime = calculateProcessingTime(taskType, in.Query)
		} else if taskType == 2 { // Fibonacci Sequence
			// Parse and execute Fibonacci calculation
			result = executeFibonacciSequence(in.Query)
			processingTime = calculateProcessingTime(taskType, in.Query)
		} else {
			// Unknown task type, use default processing time
			processingTime = time.Duration(rand.Intn(2)+1) * time.Second
			result = "Unknown task type"
		}

		// Simulate processing
		time.Sleep(processingTime)
		
		log.Printf("Worker %d: Task %d (%s) result: %s", 
			s.port, in.TaskId, taskTypeToString(taskType), result)

		// Decrement active tasks counter
		s.activeTasksMutex.Lock()
		s.activeTasks--
		newActiveTaskCount := s.activeTasks
		s.activeTasksMutex.Unlock()

		log.Printf("Worker %d: Completed task %d (%s) (remaining tasks: %d)",
			s.port, in.TaskId, taskTypeToString(taskType), newActiveTaskCount)

		// Notify leader about task completion
		conn, err := grpc.Dial("localhost:"+strconv.Itoa(s.currentLeaderPort), grpc.WithInsecure())
		if err != nil {
			log.Printf("Worker %d: Failed to connect to leader to report task completion: %v", s.port, err)
			return
		}
		defer conn.Close()

		client := pb.NewLeaderNodeClient(conn)
		_, err = client.TaskCompletionResponse(context.Background(), &pb.Task_Reply{TaskId: in.TaskId})

		if err != nil {
			log.Printf("Worker %d: Failed to report task completion for task %d: %v", s.port, in.TaskId, err)
		} else {
			log.Printf("Worker %d: Successfully reported task completion for task %d", s.port, in.TaskId)
		}

		// Report updated CPU load after task completion
		currentCpuLoad := getCPUUsage()
		newEffectiveLoad := currentCpuLoad + float64(newActiveTaskCount*10)

		s.cpuUsageMutex.Lock()
		s.cpuUsage = newEffectiveLoad
		s.cpuUsageMutex.Unlock()

		// Report to leader
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

// Helper functions for task execution

// Calculates appropriate processing time based on task complexity
func calculateProcessingTime(taskType int, query string) time.Duration {
	switch taskType {
	case 0: // Matrix Multiplication
		lines := strings.Split(query, "\n")
		if len(lines) < 2 {
			return time.Second
		}
		dimensions := strings.Split(lines[1], ",")
		if len(dimensions) < 3 {
			return time.Second
		}
		
		// Extract matrix dimensions
		rows1, _ := strconv.Atoi(dimensions[0])
		cols1, _ := strconv.Atoi(dimensions[1])
		cols2, _ := strconv.Atoi(dimensions[2])
		
		// Base time plus time proportional to matrix size
		baseTime := 500 * time.Millisecond
		sizeTime := time.Duration(rows1 * cols1 * cols2 * 5) * time.Millisecond
		return baseTime + sizeTime
		
	case 1: // Prime Factorization
		lines := strings.Split(query, "\n")
		if len(lines) < 2 {
			return time.Second
		}
		
		number, _ := strconv.Atoi(lines[1])
		// Larger numbers take more time
		baseTime := 300 * time.Millisecond
		sizeTime := time.Duration(number/1000) * time.Millisecond
		return baseTime + sizeTime
		
	case 2: // Fibonacci
		lines := strings.Split(query, "\n")
		if len(lines) < 2 {
			return time.Second
		}
		
		n, _ := strconv.Atoi(lines[1])
		// Larger Fibonacci numbers take more time
		baseTime := 200 * time.Millisecond
		sizeTime := time.Duration(n*30) * time.Millisecond
		return baseTime + sizeTime
		
	default:
		return time.Second
	}
}

// Executes matrix multiplication
func executeMatrixMultiplication(query string) string {
	lines := strings.Split(query, "\n")
	if len(lines) < 3 {
		return "Invalid matrix multiplication query format"
	}
	
	// Parse dimensions
	dimensions := strings.Split(lines[1], ",")
	if len(dimensions) < 3 {
		return "Invalid matrix dimensions"
	}
	
	rows1, err1 := strconv.Atoi(dimensions[0])
	cols1, err2 := strconv.Atoi(dimensions[1])
	cols2, err3 := strconv.Atoi(dimensions[2])
	
	if err1 != nil || err2 != nil || err3 != nil {
		return "Invalid matrix dimensions"
	}
	
	// Parse first matrix
	matrix1 := make([][]int, rows1)
	for i := 0; i < rows1; i++ {
		if i+2 >= len(lines) {
			return "Invalid matrix data"
		}
		
		values := strings.Split(lines[i+2], ",")
		if len(values) < cols1 {
			return "Invalid matrix row length"
		}
		
		matrix1[i] = make([]int, cols1)
		for j := 0; j < cols1; j++ {
			matrix1[i][j], _ = strconv.Atoi(values[j])
		}
	}
	
	// Parse second matrix
	matrix2 := make([][]int, cols1)
	for i := 0; i < cols1; i++ {
		if i+2+rows1 >= len(lines) {
			return "Invalid matrix data"
		}
		
		values := strings.Split(lines[i+2+rows1], ",")
		if len(values) < cols2 {
			return "Invalid matrix row length"
		}
		
		matrix2[i] = make([]int, cols2)
		for j := 0; j < cols2; j++ {
			matrix2[i][j], _ = strconv.Atoi(values[j])
		}
	}
	
	// Perform matrix multiplication
	result := make([][]int, rows1)
	for i := range result {
		result[i] = make([]int, cols2)
		for j := range result[i] {
			for k := 0; k < cols1; k++ {
				result[i][j] += matrix1[i][k] * matrix2[k][j]
			}
		}
	}
	
	// Format result
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Result matrix (%dx%d):\n", rows1, cols2))
	for i := range result {
		for j := range result[i] {
			builder.WriteString(fmt.Sprintf("%d", result[i][j]))
			if j < len(result[i])-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString("\n")
	}
	
	return builder.String()
}

// Executes prime factorization
func executePrimeFactorization(query string) string {
	lines := strings.Split(query, "\n")
	if len(lines) < 2 {
		return "Invalid prime factorization query format"
	}
	
	number, err := strconv.Atoi(lines[1])
	if err != nil {
		return "Invalid number for factorization"
	}
	
	// Compute prime factors
	factors := []int{}
	n := number
	
	// Check for 2 as a factor
	for n%2 == 0 {
		factors = append(factors, 2)
		n /= 2
	}
	
	// Check for odd factors
	for i := 3; i*i <= n; i += 2 {
		for n%i == 0 {
			factors = append(factors, i)
			n /= i
		}
	}
	
	// If n is a prime number greater than 2
	if n > 2 {
		factors = append(factors, n)
	}
	
	// Format result
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Prime factorization of %d: ", number))
	for i, factor := range factors {
		builder.WriteString(fmt.Sprintf("%d", factor))
		if i < len(factors)-1 {
			builder.WriteString(" × ")
		}
	}
	
	return builder.String()
}

// Executes Fibonacci sequence calculation
func executeFibonacciSequence(query string) string {
	lines := strings.Split(query, "\n")
	if len(lines) < 2 {
		return "Invalid Fibonacci query format"
	}
	
	n, err := strconv.Atoi(lines[1])
	if err != nil {
		return "Invalid number for Fibonacci calculation"
	}
	
	// Calculate the nth Fibonacci number
	fib := calculateFibonacci(n)
	
	// Format result
	return fmt.Sprintf("Fibonacci(%d) = %d", n, fib)
}

// Calculates the nth Fibonacci number
func calculateFibonacci(n int) int {
	if n <= 0 {
		return 0
	} else if n == 1 {
		return 1
	}
	
	a, b := 0, 1
	for i := 2; i <= n; i++ {
		a, b = b, a+b
	}
	
	return b
}

// Helper functions for string conversion

// Convert task type to string
func taskTypeToString(taskType int) string {
	switch taskType {
	case 0:
		return "Matrix Multiplication"
	case 1:
		return "Prime Factorization"
	case 2:
		return "Fibonacci Sequence"
	default:
		return "Unknown Task"
	}
}

// Convert priority to string
func priorityToString(priority int) string {
	switch priority {
	case 0:
		return "Low"
	case 1:
		return "Medium"
	case 2:
		return "High"
	default:
		return "Unknown"
	}
}

func (s *Leaderserver) TaskCompletionResponse(ctx context.Context, in *pb.Task_Reply) (*pb.Empty, error) {
	Leader.taskQueueMutex.Lock()
	defer Leader.taskQueueMutex.Unlock()

	taskID := int(in.TaskId)
	if _, exists := Leader.taskCompletion[taskID]; exists {
		Leader.taskCompletion[taskID] = true
		log.Printf("Task %d marked as completed", taskID)
	} else {
		log.Printf("Task %d not found in task completion map", taskID)
	}

	return &pb.Empty{}, nil
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

			dependent_task_status := true

			for _, dep := range task.dependencyList {
				if _, exists := Leader.taskCompletion[dep]; !exists {
					dependent_task_status = false
					break
				}

			}
			if !dependent_task_status {
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
			if _, exists := Leader.taskAssign[workerPort]; !exists {
				Leader.taskAssign[workerPort] = []Task{}
			}
			Leader.taskAssign[workerPort] = append(Leader.taskAssign[workerPort], task)
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
			nodeType:          "leader",
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
	Leader.timer_worker = make(map[int]time.Time)
	Leader.taskCompletion = make(map[int]bool)
	Leader.taskAssign = make(map[int][]Task)
	lastSelectedWorker = 0 // Initialize the round-robin counter
	Leader.globalMutex.Unlock()

	// Start task processor
	go processTaskQueue()
	go func() {
		for {
			time.Sleep(1 * time.Second)
			nodes_true := []int{}
			for _, port := range Leader.nodePortList {
				if port == Leader.leaderNodePort {
					nodes_true = append(nodes_true, port)
					continue
				}

				if Leader.timer_worker[port].Add(7 * time.Second).Before(time.Now()) {
					log.Printf("Worker %d not responding, removing from list", port)
					Leader.workerLoadMutex.Lock()
					delete(Leader.workerLoads, port)
					Leader.workerLoadMutex.Unlock()
					Leader.globalMutex.Lock()
					delete(Leader.timer_worker, port)
					Leader.globalMutex.Unlock()
					if tasks, exists := Leader.taskAssign[port]; exists {
						Leader.taskQueueMutex.Lock()
						Leader.taskQueue = append(Leader.taskQueue, tasks...)
						Leader.taskQueueMutex.Unlock()
					}
					Leader.globalMutex.Lock()
					delete(Leader.taskAssign, port)
					Leader.globalMutex.Unlock()
					continue
				} else {
					nodes_true = append(nodes_true, port)
				}
			}
			Leader.globalMutex.Lock()
			Leader.nodePortList = nodes_true
			Leader.globalMutex.Unlock()

		}
	}()
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
	Leader.taskCompletion[task.ID] = false
	Leader.taskQueueMutex.Unlock()

	log.Printf("Added task %d to queue (type: %d, priority: %d)", task.ID, task.TaskType, task.Priority)
	return &pb.Task_Reply{TaskId: int32(task.ID)}, nil
}

func (s *SchedulerServer) QueryClientPort(ctx context.Context, in *pb.Empty) (*pb.ClientPort, error) {

	return &pb.ClientPort{Port: int32(Leader.clientNodePort)}, nil
}

func (s *SchedulerServer) GetTaskStatus(ctx context.Context, in *pb.Task_Reply) (*pb.TaskStatus, error) {

	status := Leader.taskCompletion[int(in.TaskId)]
	return &pb.TaskStatus{Status: status}, nil
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

func (w *Node) GetServerPort(ctx context.Context, in *pb.Empty) (*pb.ServerPort, error) {

	log.Print("redirecting to leader")
	return &pb.ServerPort{Port: int32(w.currentLeaderPort)}, nil
}

func (s *Leaderserver) Heartbeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.NodeList, error) {
	Leader.globalMutex.Lock()
	defer Leader.globalMutex.Unlock()

	nodePorts := make([]int32, len(Leader.nodePortList))
	for i, port := range Leader.nodePortList {
		nodePorts[i] = int32(port)
	}
	Leader.timer_worker[int(in.Port)] = time.Now()
	log.Printf("Received heartbeat from node %d at %s", in.Port, time.Now().Format(time.RFC3339))
	return &pb.NodeList{
		NodesPort:  nodePorts,
		LeaderPort: int32(Leader.leaderNodePort),
		ClientPort: int32(Leader.clientNodePort),
		TaskId:     int32(Leader.taskIDNumber),
		TaskQueue: func() map[int32]*pb.NodeList_Task {
			taskQueue := make(map[int32]*pb.NodeList_Task)
			for _, task := range Leader.taskQueue {
				taskQueue[int32(task.ID)] = &pb.NodeList_Task{
					Id:         int32(task.ID),
					TaskType:   int32(task.TaskType),
					Priority:   int32(task.Priority),
					Query:      task.Query,
					AssignedTo: int32(task.AssignedTo),
					Status:     task.status,
					DependencyList: func() []int32 {
						deps := make([]int32, len(task.dependencyList))
						for i, dep := range task.dependencyList {
							deps[i] = int32(dep)
						}
						return deps
					}(),
				}
			}
			return taskQueue
		}(),
		WorkerLoads: func() map[int32]float32 {
			workerLoads := make(map[int32]float32)
			for port, load := range Leader.workerLoads {
				workerLoads[int32(port)] = float32(load)
			}
			return workerLoads
		}(),
		TaskCompletion: func() map[int32]bool {
			taskCompletion := make(map[int32]bool)
			for id, completed := range Leader.taskCompletion {
				taskCompletion[int32(id)] = completed
			}
			return taskCompletion
		}(),
		TaskAssign: func() map[int32]*pb.NodeList_Task {
			taskAssign := make(map[int32]*pb.NodeList_Task)
			for port, tasks := range Leader.taskAssign {
				for _, task := range tasks {
					taskAssign[int32(port)] = &pb.NodeList_Task{
						Id:         int32(task.ID),
						TaskType:   int32(task.TaskType),
						Priority:   int32(task.Priority),
						Query:      task.Query,
						AssignedTo: int32(task.AssignedTo),
						Status:     task.status,
						DependencyList: func() []int32 {
							deps := make([]int32, len(task.dependencyList))
							for i, dep := range task.dependencyList {
								deps[i] = int32(dep)
							}
							return deps
						}(),
					}
				}
			}
			return taskAssign
		}(),
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
	go func() {
		for {
			time.Sleep(1 * time.Second)
			nodes_true := []int{}
			for _, port := range Leader.nodePortList {
				if port == Leader.leaderNodePort {
					nodes_true = append(nodes_true, port)
					continue
				}

				if Leader.timer_worker[port].Add(7 * time.Second).Before(time.Now()) {
					log.Printf("Worker %d not responding, removing from list", port)
					Leader.workerLoadMutex.Lock()
					delete(Leader.workerLoads, port)
					Leader.workerLoadMutex.Unlock()
					Leader.globalMutex.Lock()
					delete(Leader.timer_worker, port)
					Leader.globalMutex.Unlock()
					if tasks, exists := Leader.taskAssign[port]; exists {
						Leader.taskQueueMutex.Lock()
						Leader.taskQueue = append(Leader.taskQueue, tasks...)
						Leader.taskQueueMutex.Unlock()
					}
					Leader.globalMutex.Lock()
					delete(Leader.taskAssign, port)
					Leader.globalMutex.Unlock()
					continue
				} else {
					nodes_true = append(nodes_true, port)
				}
			}
			Leader.globalMutex.Lock()
			Leader.nodePortList = nodes_true
			Leader.globalMutex.Unlock()

		}
	}()
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

	client := pb.NewLeaderNodeClient(conn)
	resp, err := client.GetServerPort(context.Background(), &pb.Empty{})
	if err != nil {
		conn.Close()
		conn2, err2 := grpc.Dial("localhost:"+strconv.Itoa(networkPort), grpc.WithInsecure())
		if err2 != nil {
			log.Fatalf("Failed to connect to leader: %v", errors.New("failed to connect to leader"))
		}
		// print(networkPort)
		client1 := pb.NewServerNodeClient(conn2)
		resp1, err1 := client1.GetServerPort(context.Background(), &pb.Empty{})
		// print(resp1.Port)
		conn2.Close()
		conn3, err3 := grpc.Dial("localhost:"+strconv.Itoa(int(resp1.Port)), grpc.WithInsecure())
		if err3 != nil {
			log.Fatalf("Failed to connect to lleader: %v", err1)
		}
		defer conn3.Close()
		client3 := pb.NewLeaderNodeClient(conn3)

		// if err1 != nil {
		// 	log.Fatalf("Failed to get leader port from worker: %v", err)
		// }
		resp3, err4 := client3.GetServerPort(context.Background(), &pb.Empty{})
		if err4 != nil {
			log.Fatalf("Failed to get worker port from leader: %v", err)
		}

		resp = resp3
		networkPort = int(resp1.Port)
	}
	// log.Fatalf("Failed to get worker port from leader: %v", err)

	// print("HERE")
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
				total_votes := 1
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
						total_votes++
						log.Printf("Node %d: Received vote from node %d", node.port, port)
					} else {
						total_votes++
					}
				}

				if votes > total_votes/2 {
					log.Printf("Node %d: Won election with %d votes (term %d)", node.port, votes, node.term)
					node.nodeType = "leader"
					portes := []int{}
					for _, port := range node.nodePortList {
						if port != node.localLeaderPort {
							portes = append(portes, port)
						}
					}
					node.localLeaderPort = node.port
					node.nodePortList = portes
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
			resp, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{Port: int32(node.port)})
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
			Leader.globalMutex.Lock()
			Leader.leaderNodePort = int(resp.LeaderPort)
			Leader.nodePortList = make([]int, len(resp.NodesPort))
			for i, port := range resp.NodesPort {
				Leader.nodePortList[i] = int(port)
			}
			Leader.taskIDNumber = int(resp.TaskId)
			Leader.taskQueue = []Task{}
			for id, task := range resp.TaskQueue {
				Leader.taskQueue = append(Leader.taskQueue, Task{
					ID:         int(id),
					TaskType:   int(task.TaskType),
					Priority:   int(task.Priority),
					Query:      task.Query,
					AssignedTo: int(task.AssignedTo),
					status:     task.Status,
					dependencyList: func() []int {
						deps := make([]int, len(task.DependencyList))
						for i, dep := range task.DependencyList {
							deps[i] = int(dep)
						}
						return deps
					}(),
				})
			}
			Leader.workerLoads = make(map[int]float64)
			for port, load := range resp.WorkerLoads {
				Leader.workerLoads[int(port)] = float64(load)
			}
			Leader.taskCompletion = make(map[int]bool)
			for id, completed := range resp.TaskCompletion {
				Leader.taskCompletion[int(id)] = completed
			}
			Leader.taskAssign = make(map[int][]Task)
			for port, task := range resp.TaskAssign {
				Leader.taskAssign[int(port)] = append(Leader.taskAssign[int(port)], Task{
					ID:         int(task.Id),
					TaskType:   int(task.TaskType),
					Priority:   int(task.Priority),
					Query:      task.Query,
					AssignedTo: int(task.AssignedTo),
					status:     task.Status,
					dependencyList: func() []int {
						deps := make([]int, len(task.DependencyList))
						for i, dep := range task.DependencyList {
							deps[i] = int(dep)
						}
						return deps
					}(),
				})
			}
			Leader.globalMutex.Unlock()
			conn.Close()

			time.Sleep(3 * time.Second)
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
