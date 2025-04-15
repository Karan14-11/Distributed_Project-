package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"strconv"
	"time"

	"fmt"

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
	PRIORITY_LOW    = 0
	PRIORITY_MEDIUM = 1
	PRIORITY_HIGH   = 2
)

// Function to generate a random matrix multiplication task
func generateMatrixMultiplication() string {
	// Generate random dimensions ensuring they're compatible for multiplication
	rows1 := rand.Intn(5) + 2 // 2-6 rows
	cols1 := rand.Intn(5) + 2 // 2-6 columns
	cols2 := rand.Intn(5) + 2 // 2-6 columns

	// Generate first matrix
	matrix1 := make([][]int, rows1)
	for i := range matrix1 {
		matrix1[i] = make([]int, cols1)
		for j := range matrix1[i] {
			matrix1[i][j] = rand.Intn(10) // 0-9 values
		}
	}

	// Generate second matrix
	matrix2 := make([][]int, cols1)
	for i := range matrix2 {
		matrix2[i] = make([]int, cols2)
		for j := range matrix2[i] {
			matrix2[i][j] = rand.Intn(10) // 0-9 values
		}
	}

	// Convert to string format
	query := "MATRIX_MULTIPLY\n"
	query += strconv.Itoa(rows1) + "," + strconv.Itoa(cols1) + "," + strconv.Itoa(cols2) + "\n"

	// First matrix
	for i := range matrix1 {
		for j := range matrix1[i] {
			query += strconv.Itoa(matrix1[i][j])
			if j < len(matrix1[i])-1 {
				query += ","
			}
		}
		query += "\n"
	}

	// Second matrix
	for i := range matrix2 {
		for j := range matrix2[i] {
			query += strconv.Itoa(matrix2[i][j])
			if j < len(matrix2[i])-1 {
				query += ","
			}
		}
		query += "\n"
	}

	return query
}

// Function to generate a prime factorization task
func generatePrimeFactorization() string {
	// Generate a random large number to factorize
	number := rand.Intn(90000) + 10000 // 10000-99999
	return "PRIME_FACTORIZE\n" + strconv.Itoa(number)
}

// Function to generate a Fibonacci sequence task
func generateFibonacciSequence() string {
	// Generate a random n for Fibonacci sequence
	n := rand.Intn(30) + 10 // Calculate 10th to 39th Fibonacci number
	return "FIBONACCI\n" + strconv.Itoa(n)
}

func main() {
	// Seed the random number generator

	// Parse client port as a flag
	clientPort := flag.String("port", "7000", "The port on which the client connects to the scheduler")
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	// Connect to the scheduler
	conn, err := grpc.Dial("localhost:"+*clientPort, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewSchedulerClient(conn)

	// infinite loop
	for i := 0; i < 100; i++ {

		var action int
		log.Println("Enter 1 to submit a new task or 2 to check the status of a previous task:")
		_, err := fmt.Scan(&action)
		if err != nil || (action != 1 && action != 2) {
			log.Printf("Invalid input: %v", err)
			continue
		}

		if action == 2 {
			var taskId int
			log.Println("Enter the Task ID to check the status:")
			_, err := fmt.Scan(&taskId)
			if err != nil {
				log.Printf("Invalid input: %v", err)
				continue
			}

			statusResp, err := client.GetTaskStatus(context.Background(), &pb.Task_Reply{
				TaskId: int32(taskId),
			})
			if err != nil {
				log.Printf("Failed to check status for Task ID %d: %v", taskId, err)
			} else {
				if statusResp.Status {
					log.Printf("Task ID %d resulted in %d", taskId, statusResp.Result)
				} else {
					log.Printf("Task ID %d is not completed", taskId)
				}
			}
			continue
		}

		// Determine task type (0, 1, or 2)
		var taskType int
		log.Println("Enter the task type (0 for  Multiplication, 1 for Addition, 2 for Fibonacci Sequence):")
		_, err = fmt.Scan(&taskType)
		if err != nil || (taskType != MULTIPLICATION && taskType != ADDITION && taskType != TASK_FIBONACCI_SEQUENCE) {
			log.Printf("Invalid task type: %v", err)
			continue
		}

		var priority_str string
		log.Printf("Enter the priority for task %d (%s):", i, taskTypeToString(taskType))
		log.Printf("Low or Medium or High")
		_, err = fmt.Scan(&priority_str)
		if err != nil {
			log.Printf("Invalid input: %v", err)
			continue
		}

		var priority int

		if priority_str == "low" || priority_str == "Low" {
			priority = PRIORITY_LOW
		} else if priority_str == "medium" || priority_str == "Medium" {
			priority = PRIORITY_MEDIUM
		} else if priority_str == "high" || priority_str == "High" {
			priority = PRIORITY_HIGH
		} else {
			log.Printf("Invalid priority level: %s", priority_str)
			continue
		}

		// Send task to scheduler
		var inputNumber int
		log.Printf("Enter a number for task (size of operation) %d (%s):", i, taskTypeToString(taskType))
		_, err = fmt.Scan(&inputNumber)
		if err != nil {
			log.Printf("Invalid input: %v", err)
			continue
		}

		// Take input for the size of the array
		var arraySize int
		log.Printf("Enter the dependent tasks %d (%s):", i, taskTypeToString(taskType))
		_, err = fmt.Scan(&arraySize)
		if err != nil || arraySize < 0 {
			log.Printf("Invalid array size: %v", err)
			continue
		}

		// Take input for the array elements
		array := make([]int, arraySize)
		log.Printf("Enter %d task ids of dependent tasks:", arraySize)
		for j := 0; j < arraySize; j++ {
			_, err = fmt.Scan(&array[j])
			if err != nil {
				log.Printf("Invalid input for array element %d: %v", j, err)
				continue
			}
		}

		// Convert array to a comma-separated string

		query := strconv.Itoa(inputNumber)

		// Convert array from []int to []int32
		dependencyList := make([]int32, len(array))
		for i, v := range array {
			dependencyList[i] = int32(v)
		}

		resp, err := client.QueryTask(context.Background(), &pb.Task_Query{
			TaskType:       int32(taskType),
			DataQuery:      query,
			Priority:       int32(priority),
			DependencyList: dependencyList,
		})

		if err != nil {
			log.Printf("Failed to send task %d (%s): %v", i, taskTypeToString(taskType), err)
		} else {
			log.Printf("Sent task %d (%s) with priority %s, got ID: %d",
				i,
				taskTypeToString(taskType),
				priorityToString(priority),
				resp.TaskId)
		}

		// Wait a bit before sending next task
	}
}

// Helper function to convert task type to string
func taskTypeToString(taskType int) string {
	switch taskType {
	case MULTIPLICATION:
		return "Matrix Multiplication"
	case ADDITION:
		return "Prime Factorization"
	case TASK_FIBONACCI_SEQUENCE:
		return "Fibonacci Sequence"
	default:
		return "Unknown Task"
	}
}

// Helper function to convert priority to string
func priorityToString(priority int) string {
	switch priority {
	case PRIORITY_LOW:
		return "Low"
	case PRIORITY_MEDIUM:
		return "Medium"
	case PRIORITY_HIGH:
		return "High"
	default:
		return "Unknown"
	}
}
