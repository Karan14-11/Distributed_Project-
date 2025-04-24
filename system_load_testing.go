package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
	"os"
	"encoding/csv"
	"strconv"

	pb "github.com/Karan14-11/Distributed_Project-/proto"
	"google.golang.org/grpc"
)

// Constants for task types
const (
	MULTIPLICATION = 0
	ADDITION       = 1
	FIBONACCI      = 2
)

func main() {
	// Parse command line flags
	clientPort := flag.String("port", "50021", "The port on which to connect to the scheduler")
	outputFile := flag.String("output", "load_test_results.csv", "Output CSV file name")
	flag.Parse()

	// Connect to server
	conn, err := grpc.Dial("localhost:"+*clientPort, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewSchedulerClient(conn)

	// Create CSV file for results
	file, err := os.Create(*outputFile)
	if err != nil {
		log.Fatalf("Could not create output file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	writer.Write([]string{"ConcurrentClients", "TotalTasks", "SuccessRate", "AvgResponseTime", "Throughput"})

	// Test with increasing load
	testLoads := []int{10, 50, 100, 200, 300, 500}
	
	fmt.Println("Starting load test with increasing concurrent clients...")
	fmt.Println("---------------------------------------------------")
	fmt.Printf("%-20s %-15s %-15s %-20s %-15s\n", 
		"Concurrent Clients", "Total Tasks", "Success Rate", "Avg Response Time", "Throughput")
	fmt.Println("---------------------------------------------------")

	for _, clientCount := range testLoads {
		// Run the test with current client count
		totalTasks := clientCount * 3 // Each client submits 3 task types
		successCount, avgResponseTime, totalTime := runTest(client, clientCount)
		
		successRate := float64(successCount) / float64(totalTasks) * 100
		throughput := float64(successCount) / totalTime.Seconds()
		
		// Print results
		fmt.Printf("%-20d %-15d %-15.2f%% %-20.2fms %-15.2f\n", 
			clientCount, totalTasks, successRate, avgResponseTime, throughput)
		
		// Write results to CSV
		writer.Write([]string{
			strconv.Itoa(clientCount),
			strconv.Itoa(totalTasks),
			fmt.Sprintf("%.2f", successRate),
			fmt.Sprintf("%.2f", avgResponseTime),
			fmt.Sprintf("%.2f", throughput),
		})
		writer.Flush()
		
		// Small pause between tests
		time.Sleep(5 * time.Second)
	}
	
	fmt.Println("---------------------------------------------------")
	fmt.Println("Load test completed. Results saved to", *outputFile)
}

func runTest(client pb.SchedulerClient, clientCount int) (int, float64, time.Duration) {
	rand.Seed(time.Now().UnixNano())
	
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	var totalResponseTime float64
	
	startTime := time.Now()
	
	// Start clients
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			
			// Each client submits one of each task type
			for taskType := 0; taskType <= 2; taskType++ {
				// Choose task size based on type
				var taskSize int
				switch taskType {
				case MULTIPLICATION:
					taskSize = rand.Intn(15) + 10 // 10-24
				case ADDITION:
					taskSize = rand.Intn(30) + 20 // 20-49
				case FIBONACCI:
					taskSize = rand.Intn(10) + 15 // 15-24
				}
				
				// Submit task and measure response time
				taskStartTime := time.Now()
				resp, err := client.QueryTask(context.Background(), &pb.Task_Query{
					TaskType:       int32(taskType),
					DataQuery:      fmt.Sprintf("%d", taskSize),
					Priority:       int32(2), // High priority
					DependencyList: []int32{},
				})
				
				if err != nil {
					continue
				}
				
				responseTime := time.Since(taskStartTime).Milliseconds()
				
				// Wait for task to complete
				completed := false
				timeout := time.After(30 * time.Second)
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				
				for !completed {
					select {
					case <-ticker.C:
						statusResp, err := client.GetTaskStatus(context.Background(), &pb.Task_Reply{
							TaskId: resp.TaskId,
						})
						
						if err != nil {
							continue
						}
						
						if statusResp.Status {
							mu.Lock()
							successCount++
							totalResponseTime += float64(responseTime)
							mu.Unlock()
							completed = true
						}
					case <-timeout:
						completed = true
					}
				}
			}
		}(i)
		
		// Add small delay between client starts to avoid thundering herd
		if i % 10 == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	
	// Wait for all clients to finish
	wg.Wait()
	
	totalTime := time.Since(startTime)
	
	var avgResponseTime float64
	if successCount > 0 {
		avgResponseTime = totalResponseTime / float64(successCount)
	}
	
	return successCount, avgResponseTime, totalTime
}