#!/bin/bash

# Loop to kill servers with ports starting from 50051 to 50150
for ((port=5000; port<5100; port++)); do
    echo "Killing server on port $port"
    # Replace the following line with the actual command to kill the server
    bash server_kill.sh $port
    # sleep 1
done