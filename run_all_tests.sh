#!/bin/bash

# Script to run performance tests for different client counts
# and generate comparative analysis

# Make sure the script stops on any error
set -e

# Create directories for results
mkdir -p test_results
mkdir -p comparative_graphs

# Function to check if the leader node is running
check_leader() {
    echo "Checking leader node status..."
    if nc -z localhost 5000 2>/dev/null; then
        echo "Leader node is running."
        return 0
    else
        echo "Leader node is not running. Please start it with: make first"
        return 1
    fi
}

# Function to check if additional worker nodes are running
check_workers() {
    min_count=$1
    worker_count=0
    
    # Check ports 5001-5010 for workers
    for port in $(seq 5001 5010); do
        if nc -z localhost $port 2>/dev/null; then
            worker_count=$((worker_count + 1))
        fi
    done
    
    echo "Found $worker_count worker nodes."
    
    if [ $worker_count -lt $min_count ]; then
        echo "Warning: For better testing, at least $min_count worker nodes are recommended."
        echo "Start more worker nodes with: make server"
        echo "Continue anyway? (y/n)"
        read answer
        if [ "$answer" != "y" ]; then
            echo "Aborting."
            return 1
        fi
    fi
    
    return 0
}

# Function to run a single test
run_test() {
    clients=$1
    echo ""
    echo "==============================================="
    echo "Running test with $clients clients"
    echo "==============================================="
    
    # Run the test
    go run batch_testing.go -port=50021 -clients=$clients
    
    # Wait to let the system stabilize between tests
    echo "Waiting for system to stabilize..."
    sleep 15
}

# Main script execution

echo "Distributed System Performance Test Suite"
echo "========================================="

# Check infrastructure
if ! check_leader; then
    exit 1
fi

if ! check_workers 3; then
    echo "Continuing with available workers..."
fi

# Welcome message
echo ""
echo "This script will run performance tests with 100, 500, and 1000 clients"
echo "Each client will submit 3 high priority tasks of different types"
echo ""
echo "This test will take some time to complete. Continue? (y/n)"
read answer
if [ "$answer" != "y" ]; then
    echo "Test aborted."
    exit 0
fi

# Run tests with different client counts
echo "Starting tests sequence..."

# Test with 100 clients
run_test 100

# Test with 500 clients
run_test 500

# Test with 1000 clients
run_test 1000

echo "All tests completed!"

# # Run comparative analysis
# echo ""
# echo "Generating comparative analysis..."
# python modified_comparative_analysis.py

echo ""
echo "Performance testing complete!"
# echo "Check the 'comparative_graphs' directory for visualization results"
# echo "Individual test results are in the 'test_results' directory"