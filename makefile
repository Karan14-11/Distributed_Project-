PROTO_DIR = proto
CLIENT_DIR = client
SERVER_DIR = server


PROTO_FILE = $(PROTO_DIR)/scheduler.proto 
PROTO_OUT_DIR = .

GO_FLAGS = -I=. --go_out=$(PROTO_OUT_DIR) --go_opt=paths=source_relative \
           --go-grpc_out=$(PROTO_OUT_DIR) --go-grpc_opt=paths=source_relative

SERVER_PORT = 50050
LOADB_PORT = 50059

.PHONY: proto server client clean


proto:
	rm -rf $(PROTO_DIR)/*.pb.go  # Remove old files
	protoc $(GO_FLAGS) $(PROTO_FILE)

backend_server:
	go run $(SERVER_DIR)/main.go --port=$(SERVER_PORT)

mr:
	go run server/main.go 2

client:
	go run $(CLIENT_DIR)/main.go

clean:
	rm -f $(PROTO_DIR)/*.pb.go

