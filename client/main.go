package main

import (
	"context"
	"log"
	"time"
	"strconv"
	pb "github.com/Karan14-11/Distributed_Project-/proto"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:50060", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewSchedulerClient(conn)

	for i := 0; i < 20; i++ {
		resp, err := client.QueryTask(context.Background(), &pb.Task_Query{
			TaskType:   int32(i % 3),
			DataQuery:  "test query " + strconv.Itoa(i),
			Priority:   int32(i % 5),
		})
		if err != nil {
			log.Printf("Failed to send task %d: %v", i, err)
		} else {
			log.Printf("Sent task %d, got ID: %d", i, resp.TaskId)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
