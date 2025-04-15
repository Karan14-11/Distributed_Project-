package main
import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"time"

	pb "github.com/Karan14-11/Distributed_Project-/proto"
	"google.golang.org/grpc"
)

// Constants for task types
const (
	TASK_MATRIX_MULTIPLICATION = 0
	TASK_PRIME_FACTORIZATION   = 1
	TASK_FIBONACCI_SEQUENCE    = 2
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
	rows1 := rand.Intn(5) + 2  // 2-6 rows
	cols1 := rand.Intn(5) + 2  // 2-6 columns
	cols2 := rand.Intn(5) + 2  // 2-6 columns
	
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
	rand.Seed(time.Now().UnixNano())

	// Connect to the scheduler
	conn, err := grpc.Dial("localhost:7000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewSchedulerClient(conn)

	// Submit a mix of tasks
	for i := 0; i < 20; i++ {
		// Determine task type (0, 1, or 2)
		taskType := i % 3
		
		// Determine priority level (0, 1, or 2)
		priority := rand.Intn(3) // Low (0), Medium (1), High (2)
		
		// Generate query based on task type
		var query string
		switch taskType {
		case TASK_MATRIX_MULTIPLICATION:
			query = generateMatrixMultiplication()
		case TASK_PRIME_FACTORIZATION:
			query = generatePrimeFactorization()
		case TASK_FIBONACCI_SEQUENCE:
			query = generateFibonacciSequence()
		}
		
		// Send task to scheduler
		resp, err := client.QueryTask(context.Background(), &pb.Task_Query{
			TaskType:   int32(taskType),
			DataQuery:  query,
			Priority:   int32(priority),
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
		time.Sleep(500 * time.Millisecond)
	}
}

// Helper function to convert task type to string
func taskTypeToString(taskType int) string {
	switch taskType {
	case TASK_MATRIX_MULTIPLICATION:
		return "Matrix Multiplication"
	case TASK_PRIME_FACTORIZATION:
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
