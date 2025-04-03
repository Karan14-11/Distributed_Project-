// package main

// import (
// 	"context"
// 	pb "github.com/Karan14-11/distributed_systems_A2/Q2/proto"
// 	"google.golang.org/grpc"
// 	"log"
// 	"os"
// 	"strconv"
// )

// func main() {
// 	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
// 	if err != nil {
// 		log.Fatalf("Failed to connect to Load Balancer: %v", err)
// 	}
// 	mrClient := pb.NewMapReduceClient(conn)
// 	queryInt, err := strconv.Atoi(os.Args[1])
// 	if err != nil {
// 		log.Fatalf("Failed to convert query to integer: %v", err)
// 	}
// 	_, err = mrClient.Map(context.Background(), &pb.Query{Query: int32(queryInt), InputDir: os.Args[2], OutputFile: os.Args[3]})
// 	if err != nil {
// 		log.Fatalf("Failed to get server: %v", err)
// 	}
// 	conn.Close()

// 	log.Println("Client request handled successfully")
// }
