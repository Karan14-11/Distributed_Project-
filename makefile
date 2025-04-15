PROTO_DIR = proto
SERVER_DIR = server


PROTO_FILE = $(PROTO_DIR)/scheduler.proto 
PROTO_OUT_DIR = .

GO_FLAGS = -I=. --go_out=$(PROTO_OUT_DIR) --go_opt=paths=source_relative \
           --go-grpc_out=$(PROTO_OUT_DIR) --go-grpc_opt=paths=source_relative


.PHONY: proto server client clean

proto:
	rm -rf $(PROTO_DIR)/*.pb.go  # Remove old files
	protoc $(GO_FLAGS) $(PROTO_FILE)

first:
	go run ./server/main.go -first_node=true -network_port=50051 -client_port=50021

server:
	go run ./server/main.go -network_port=50052

client:
	go run ./client/main.go -port=50021


clean:
	rm -f $(PROTO_DIR)/*.pb.go

