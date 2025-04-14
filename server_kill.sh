#!/bin/bash

# Check if at least one port is provided
if [ "$#" -eq 0 ]; then
    echo "Usage: $0 <port1> [port2] ..."
    exit 1
fi

for port in "$@"; do
    pid=$(lsof -t -i:$port)

    if [ -n "$pid" ]; then
        echo "Stopping process on port $port (PID: $pid)..."
        kill -9 $pid
    else
        echo "No process found on port $port."
    fi
done

echo "Done."