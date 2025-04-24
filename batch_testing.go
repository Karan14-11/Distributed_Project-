package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
	"encoding/csv"
	"strconv"

	pb "github.com/Karan14-11/Distributed_Project-/proto"
	"google.golang.org/grpc"
)

// Constants for task types
const (
	MULTIPLICATION          = 0
	ADDITION                = 1
	TASK_FIBONACCI_SEQUENCE = 2
)

// Constants for priority levels
const (
	PRIORITY_HIGH   = 2
)

type TaskResult struct {
	ClientID       int
	TaskID         int32
	TaskType       int
	SubmissionTime time.Time
	ResponseTime   time.Duration // Time to get task ID
	ExecutionTime  time.Duration // Time to complete task
	CompletionTime time.Time
}

func main() {
	// Parse command line flags
	clientPort := flag.String("port", "50021", "The port on which to connect to the scheduler")
	numClients := flag.Int("clients", 100, "Number of clients to simulate")
	outputFile := flag.String("output", "", "Output CSV file name (default: performance_results_<numClients>.csv)")
	flag.Parse()

	// Set default output file if not specified
	if *outputFile == "" {
		*outputFile = fmt.Sprintf("performance_results_%d.csv", *numClients)
	}

	// Make sure the test_results directory exists
	os.MkdirAll("test_results", 0755)
	
	// Prepend the directory to the output filename
	*outputFile = "test_results/" + *outputFile

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Set up connection to server
	conn, err := grpc.Dial("localhost:"+*clientPort, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewSchedulerClient(conn)

	// Initialize results channel and wait group
	var wg sync.WaitGroup
	resultsChan := make(chan TaskResult, *numClients*3) // 3 task types per client
	
	fmt.Printf("Starting performance test with %d clients\n", *numClients)
	fmt.Printf("Results will be saved to %s\n", *outputFile)

	// Record test start time
	testStartTime := time.Now()
	
	// Start the clients
	for i := 0; i < *numClients; i++ {
		wg.Add(1)
		
		go func(clientID int) {
			defer wg.Done()
			
			// Each client submits all three task types (always HIGH priority)
			for taskType := 0; taskType <= 2; taskType++ {
				// Generate random input number based on task type
				var inputNumber int
				switch taskType {
				case MULTIPLICATION:
					inputNumber = rand.Intn(20) + 10 // 10-29
				case ADDITION:
					inputNumber = rand.Intn(50) + 50 // 50-99
				case TASK_FIBONACCI_SEQUENCE:
					inputNumber = rand.Intn(20) + 5 // 5-24
				}
				
				// Record submission time
				submissionTime := time.Now()
				
				// Submit the task
				resp, err := client.QueryTask(context.Background(), &pb.Task_Query{
					TaskType:       int32(taskType),
					DataQuery:      fmt.Sprintf("%d", inputNumber),
					Priority:       int32(PRIORITY_HIGH),
					DependencyList: []int32{}, // No dependencies for this test
				})
				
				if err != nil {
					log.Printf("Client %d failed to submit task type %d: %v", clientID, taskType, err)
					continue
				}
				
				// Calculate response time (time to get task ID)
				responseTime := time.Since(submissionTime)
				
				taskID := resp.TaskId
				log.Printf("Client %d: Task %d (type %d) submitted in %v", clientID, taskID, taskType, responseTime)
				
				// Set a timeout to avoid infinite wait
				timeout := time.After(5 * time.Minute)
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				
				// Poll until task completes or timeout
				taskCompleted := false
				var completionTime time.Time
				
				for !taskCompleted {
					select {
					case <-ticker.C:
						// Check task status
						statusResp, err := client.GetTaskStatus(context.Background(), &pb.Task_Reply{
							TaskId: taskID,
						})
						
						if err != nil {
							log.Printf("Client %d: Error checking status for task %d: %v", clientID, taskID, err)
							continue
						}
						
						if statusResp.Status {
							// Task has completed
							completionTime = time.Now()
							executionTime := completionTime.Sub(submissionTime)
							
							result := TaskResult{
								ClientID:       clientID,
								TaskID:         taskID,
								TaskType:       taskType,
								SubmissionTime: submissionTime,
								ResponseTime:   responseTime,
								ExecutionTime:  executionTime,
								CompletionTime: completionTime,
							}
							
							resultsChan <- result
							taskCompleted = true
							
							log.Printf("Client %d: Task %d (type %d) completed in %v", 
								clientID, taskID, taskType, executionTime)
						}
					case <-timeout:
						log.Printf("Client %d: Timeout waiting for task %d (type %d)", clientID, taskID, taskType)
						taskCompleted = true // Exit the loop even though task didn't complete
					}
				}
			}
		}(i)
	}
	
	// Wait for all clients to finish and close the results channel
	go func() {
		wg.Wait()
		close(resultsChan)
		fmt.Println("All client tasks submitted and monitored. Finalizing results...")
	}()
	
	// Collect results and write to CSV
	file, err := os.Create(*outputFile)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer file.Close()
	
	csvWriter := csv.NewWriter(file)
	defer csvWriter.Flush()
	
	// Write CSV header
	csvWriter.Write([]string{
		"ClientID",
		"TaskID",
		"TaskType",
		"TaskTypeName",
		"SubmissionTime",
		"ResponseTime(ms)",
		"ExecutionTime(ms)",
		"CompletionTime",
	})
	
	// Process and save results
	var results []TaskResult
	for result := range resultsChan {
		results = append(results, result)
		
		// Map task type to name
		taskTypeName := "Unknown"
		switch result.TaskType {
		case MULTIPLICATION:
			taskTypeName = "Multiplication"
		case ADDITION:
			taskTypeName = "Addition"
		case TASK_FIBONACCI_SEQUENCE:
			taskTypeName = "Fibonacci"
		}
		
		// Write to CSV
		csvWriter.Write([]string{
			strconv.Itoa(result.ClientID),
			strconv.Itoa(int(result.TaskID)),
			strconv.Itoa(result.TaskType),
			taskTypeName,
			result.SubmissionTime.Format(time.RFC3339Nano),
			strconv.FormatFloat(float64(result.ResponseTime.Milliseconds()), 'f', 2, 64),
			strconv.FormatFloat(float64(result.ExecutionTime.Milliseconds()), 'f', 2, 64),
			result.CompletionTime.Format(time.RFC3339Nano),
		})
		csvWriter.Flush()
	}
	
	// Calculate final statistics
	testDuration := time.Since(testStartTime)
	tasksCompleted := len(results)
	
	var totalResponseTime time.Duration
	var totalExecutionTime time.Duration
	
	for _, result := range results {
		totalResponseTime += result.ResponseTime
		totalExecutionTime += result.ExecutionTime
	}
	
	avgResponseTime := float64(0)
	avgExecutionTime := float64(0)
	
	if tasksCompleted > 0 {
		avgResponseTime = float64(totalResponseTime.Milliseconds()) / float64(tasksCompleted)
		avgExecutionTime = float64(totalExecutionTime.Milliseconds()) / float64(tasksCompleted)
	}
	
	throughput := float64(tasksCompleted) / testDuration.Seconds()
	
	// Print summary to console
	fmt.Println("\n========== TEST SUMMARY ==========")
	fmt.Printf("Test Duration: %v\n", testDuration)
	fmt.Printf("Clients: %d\n", *numClients)
	fmt.Printf("Tasks Completed: %d of %d\n", tasksCompleted, *numClients*3)
	fmt.Printf("Average Response Time: %.2f ms\n", avgResponseTime)
	fmt.Printf("Average Execution Time: %.2f ms\n", avgExecutionTime)
	fmt.Printf("Throughput: %.2f tasks/second\n", throughput)
	fmt.Println("===================================")
	
	// Write summary to a separate file
	summaryFile, err := os.Create(*outputFile + ".summary.txt")
	if err != nil {
		log.Printf("Failed to create summary file: %v", err)
	} else {
		defer summaryFile.Close()
		
		fmt.Fprintf(summaryFile, "Test Summary - %d Clients\n", *numClients)
		fmt.Fprintf(summaryFile, "========================================\n")
		fmt.Fprintf(summaryFile, "Test Start Time: %s\n", testStartTime.Format(time.RFC3339))
		fmt.Fprintf(summaryFile, "Test Duration: %v\n", testDuration)
		fmt.Fprintf(summaryFile, "Clients: %d\n", *numClients)
		fmt.Fprintf(summaryFile, "Tasks Completed: %d of %d\n", tasksCompleted, *numClients*3)
		fmt.Fprintf(summaryFile, "Average Response Time: %.2f ms\n", avgResponseTime)
		fmt.Fprintf(summaryFile, "Average Execution Time: %.2f ms\n", avgExecutionTime)
		fmt.Fprintf(summaryFile, "Throughput: %.2f tasks/second\n", throughput)
		
		// Add task type breakdown
		taskTypeCount := make(map[int]int)
		taskTypeResponseTimes := make(map[int]time.Duration)
		taskTypeExecutionTimes := make(map[int]time.Duration)
		
		for _, result := range results {
			taskTypeCount[result.TaskType]++
			taskTypeResponseTimes[result.TaskType] += result.ResponseTime
			taskTypeExecutionTimes[result.TaskType] += result.ExecutionTime
		}
		
		fmt.Fprintf(summaryFile, "\nTask Type Breakdown:\n")
		fmt.Fprintf(summaryFile, "--------------------\n")
		
		for taskType := 0; taskType <= 2; taskType++ {
			taskTypeName := "Unknown"
			switch taskType {
			case MULTIPLICATION:
				taskTypeName = "Multiplication"
			case ADDITION:
				taskTypeName = "Addition"
			case TASK_FIBONACCI_SEQUENCE:
				taskTypeName = "Fibonacci"
			}
			
			count := taskTypeCount[taskType]
			
			var avgRespTime, avgExecTime float64
			if count > 0 {
				avgRespTime = float64(taskTypeResponseTimes[taskType].Milliseconds()) / float64(count)
				avgExecTime = float64(taskTypeExecutionTimes[taskType].Milliseconds()) / float64(count)
			}
			
			fmt.Fprintf(summaryFile, "Type %d (%s):\n", taskType, taskTypeName)
			fmt.Fprintf(summaryFile, "  Count: %d\n", count)
			fmt.Fprintf(summaryFile, "  Avg Response Time: %.2f ms\n", avgRespTime)
			fmt.Fprintf(summaryFile, "  Avg Execution Time: %.2f ms\n", avgExecTime)
		}
	}
	
	fmt.Printf("Results saved to %s\n", *outputFile)
}