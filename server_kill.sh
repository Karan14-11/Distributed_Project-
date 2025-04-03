#!/bin/bash

# Number of servers to stop
num_servers=1
start_port=50052  # First server port

# Loop to find and kill processes running on these ports
for ((i=0; i<num_servers; i++)); do
    port=$((start_port + i))
    
    # Find the process using the port
    pid=$(lsof -t -i:$port)

    if [ -n "$pid" ]; then
        echo "Stopping server on port $port (PID: $pid)..."
        kill -9 $pid
    else
        echo "No server found running on port $port."
    fi
done

echo "All servers stopped."