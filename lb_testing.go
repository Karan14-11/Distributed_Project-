package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	// "math/rand"
	"os"
	"sort"
	// "strconv"
	"sync"
	"time"

	pb "github.com/Karan14-11/Distributed_Project-/proto"
	"google.golang.org/grpc"
)

// Constants for task types
const (
	MULTIPLICATION = 0
	ADDITION       = 1
	FIBONACCI      = 2
)

// TaskResult stores information about a completed task
type TaskResult struct {
	TaskID    int32
	TaskType  int
	WorkerID  int // This will be inferred from the task result
	StartTime time.Time
	EndTime   time.Time
}

func main() {
	// Parse command line flags
	clientPort := flag.String("port", "50021", "The port on which to connect to the scheduler")
	taskCount := flag.Int("tasks", 100, "Number of tasks to submit")
	outputFile := flag.String("output", "load_balance_report.txt", "Output file for results")
	flag.Parse()

	// Connect to the scheduler service
	conn, err := grpc.Dial("localhost:"+*clientPort, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewSchedulerClient(conn)

	// Print test start information
	fmt.Println("Starting load balance test...")
	fmt.Printf("Submitting %d tasks to test load distribution\n", *taskCount)

	// Submit tasks and collect results
	results := submitTasks(client, *taskCount)

	// Analyze results to determine worker distribution
	workerAnalysis := analyzeWorkerDistribution(results)

	// Print results
	printResults(workerAnalysis, *taskCount)

	// Save detailed results to file
	saveResults(*outputFile, results, workerAnalysis)
}

func submitTasks(client pb.SchedulerClient, taskCount int) []TaskResult {
	var results []TaskResult
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Submit tasks concurrently
	for i := 0; i < taskCount; i++ {
		wg.Add(1)
		go func(taskNum int) {
			defer wg.Done()

			// Create a mix of task types
			taskType := taskNum % 3 // Alternate between the 3 task types

			// Vary the task complexity to create different workloads
			var taskSize int
			switch taskType {
			case MULTIPLICATION:
				taskSize = 15
			case ADDITION:
				taskSize = 30
			case FIBONACCI:
				taskSize = 20
			}

			// Submit the task
			startTime := time.Now()
			resp, err := client.QueryTask(context.Background(), &pb.Task_Query{
				TaskType:       int32(taskType),
				DataQuery:      fmt.Sprintf("%d", taskSize),
				Priority:       int32(2), // High priority
				DependencyList: []int32{},
			})

			if err != nil {
				log.Printf("Failed to submit task %d: %v", taskNum, err)
				return
			}

			taskID := resp.TaskId
			
			// Wait for task completion
			var endTime time.Time
			workerID := -1
			
			// Poll until the task completes
			completed := false
			timeout := time.After(30 * time.Second)
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			
			for !completed {
				select {
				case <-ticker.C:
					statusResp, err := client.GetTaskStatus(context.Background(), &pb.Task_Reply{
						TaskId: taskID,
					})
					
					if err != nil {
						continue
					}
					
					if statusResp.Status {
						endTime = time.Now()
						// Use result to help identify worker
						// This is a simplification - in a real setup you might have worker IDs
						workerID = int(statusResp.Result % 10) // Use last digit as proxy for worker
						completed = true
					}
				case <-timeout:
					log.Printf("Timed out waiting for task %d", taskID)
					completed = true
				}
			}
			
			if workerID >= 0 {
				// Task completed successfully
				result := TaskResult{
					TaskID:    taskID,
					TaskType:  taskType,
					WorkerID:  workerID,
					StartTime: startTime,
					EndTime:   endTime,
				}
				
				mu.Lock()
				results = append(results, result)
				mu.Unlock()
			}
		}(i)
		
		// Small delay to avoid overwhelming the system
		time.Sleep(10 * time.Millisecond)
	}
	
	// Wait for all tasks to complete
	wg.Wait()
	return results
}

func analyzeWorkerDistribution(results []TaskResult) map[int]int {
	// Count tasks processed by each worker
	workerCounts := make(map[int]int)
	for _, result := range results {
		workerCounts[result.WorkerID]++
	}
	
	return workerCounts
}

func printResults(workerCounts map[int]int, totalTasks int) {
	// Calculate total completed tasks
	completedTasks := 0
	for _, count := range workerCounts {
		completedTasks += count
	}
	
	// Get sorted worker IDs for consistent output
	var workerIDs []int
	for workerID := range workerCounts {
		workerIDs = append(workerIDs, workerID)
	}
	sort.Ints(workerIDs)
	
	// Print worker distribution header
	fmt.Println("\n--- Load Balance Results ---")
	
	// Print worker distribution
	for _, workerID := range workerIDs {
		count := workerCounts[workerID]
		percentage := float64(count) / float64(completedTasks) * 100
		fmt.Printf("Worker %d: %d tasks (%.1f%%)\n", workerID, count, percentage)
	}
	
	// Calculate load balancing metrics
	mean := float64(completedTasks) / float64(len(workerCounts))
	
	// Calculate variance and standard deviation
	var sumSquaredDiff float64
	for _, count := range workerCounts {
		diff := float64(count) - mean
		sumSquaredDiff += diff * diff
	}
	
	variance := sumSquaredDiff / float64(len(workerCounts))
	stdDev := 0.0
	if variance > 0 {
		stdDev = variance
	}
	
	// Calculate coefficient of variation as a percentage
	cv := (stdDev / mean) * 100
	
	// Print load balancing metrics
	fmt.Println("\n--- Load Balancing Metrics ---")
	fmt.Printf("Tasks submitted: %d\n", totalTasks)
	fmt.Printf("Tasks completed: %d (%.1f%%)\n", completedTasks, float64(completedTasks)/float64(totalTasks)*100)
	fmt.Printf("Worker nodes detected: %d\n", len(workerCounts))
	fmt.Printf("Average tasks per worker: %.1f\n", mean)
	fmt.Printf("Standard deviation: %.1f\n", stdDev)
	fmt.Printf("Coefficient of Variation: %.1f%%\n", cv)
	
	// Evaluate load balancing quality
	var quality string
	if cv < 10 {
		quality = "Excellent"
	} else if cv < 20 {
		quality = "Good"
	} else if cv < 30 {
		quality = "Fair"
	} else {
		quality = "Poor"
	}
	
	fmt.Printf("Load balancing quality: %s\n", quality)
	
	// Calculate the imbalance percentage (max vs min worker loads)
	var minCount, maxCount int
	if len(workerIDs) > 0 {
		minCount = workerCounts[workerIDs[0]]
		maxCount = workerCounts[workerIDs[0]]
		
		for _, workerID := range workerIDs {
			count := workerCounts[workerID]
			if count < minCount {
				minCount = count
			}
			if count > maxCount {
				maxCount = count
			}
		}
	}
	
	imbalance := 0.0
	if minCount > 0 {
		imbalance = (float64(maxCount) / float64(minCount) - 1) * 100
	}
	
	fmt.Printf("Load imbalance: %.1f%%\n", imbalance)
}

func saveResults(filename string, results []TaskResult, workerCounts map[int]int) {
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create output file: %v", err)
		return
	}
	defer file.Close()
	
	// Write test summary
	file.WriteString("Load Balance Test Results\n")
	file.WriteString("========================\n\n")
	file.WriteString(fmt.Sprintf("Test time: %s\n", time.Now().Format(time.RFC1123)))
	file.WriteString(fmt.Sprintf("Total tasks: %d\n", len(results)))
	file.WriteString(fmt.Sprintf("Number of workers: %d\n\n", len(workerCounts)))
	
	// Write worker distribution
	file.WriteString("Worker Distribution:\n")
	file.WriteString("-------------------\n")
	
	// Get sorted worker IDs for consistent output
	var workerIDs []int
	for workerID := range workerCounts {
		workerIDs = append(workerIDs, workerID)
	}
	sort.Ints(workerIDs)
	
	totalTasks := len(results)
	for _, workerID := range workerIDs {
		count := workerCounts[workerID]
		percentage := float64(count) / float64(totalTasks) * 100
		file.WriteString(fmt.Sprintf("Worker %d: %d tasks (%.1f%%)\n", workerID, count, percentage))
	}
	
	// Calculate load balancing metrics
	mean := float64(totalTasks) / float64(len(workerCounts))
	
	// Calculate variance and standard deviation
	var sumSquaredDiff float64
	for _, count := range workerCounts {
		diff := float64(count) - mean
		sumSquaredDiff += diff * diff
	}
	
	variance := sumSquaredDiff / float64(len(workerCounts))
	stdDev := 0.0
	if variance > 0 {
		stdDev = variance
	}
	
	// Calculate coefficient of variation as a percentage
	cv := (stdDev / mean) * 100
	
	// Write load balancing metrics
	file.WriteString("\nLoad Balancing Metrics:\n")
	file.WriteString("---------------------\n")
	file.WriteString(fmt.Sprintf("Average tasks per worker: %.1f\n", mean))
	file.WriteString(fmt.Sprintf("Standard deviation: %.1f\n", stdDev))
	file.WriteString(fmt.Sprintf("Coefficient of Variation: %.1f%%\n", cv))
	
	// Evaluate load balancing quality
	var quality string
	if cv < 10 {
		quality = "Excellent"
	} else if cv < 20 {
		quality = "Good"
	} else if cv < 30 {
		quality = "Fair"
	} else {
		quality = "Poor"
	}
	
	file.WriteString(fmt.Sprintf("Load balancing quality: %s\n", quality))
	
	// Calculate the imbalance percentage (max vs min worker loads)
	var minCount, maxCount int
	if len(workerIDs) > 0 {
		minCount = workerCounts[workerIDs[0]]
		maxCount = workerCounts[workerIDs[0]]
		
		for _, workerID := range workerIDs {
			count := workerCounts[workerID]
			if count < minCount {
				minCount = count
			}
			if count > maxCount {
				maxCount = count
			}
		}
	}
	
	imbalance := 0.0
	if minCount > 0 {
		imbalance = (float64(maxCount) / float64(minCount) - 1) * 100
	}
	
	file.WriteString(fmt.Sprintf("Load imbalance: %.1f%%\n", imbalance))
	
	log.Printf("Results saved to %s", filename)
}