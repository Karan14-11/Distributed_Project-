#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}==== Distributed Task Scheduling System - Basic Test Suite ====${NC}"

# Check if ports are in use and kill them if needed
function check_ports() {
    echo -e "${YELLOW}Checking if test ports are in use...${NC}"
    ./server_kill.sh 5000 5001 5002 5003
}

# Start the leader node
function start_leader() {
    echo -e "${YELLOW}Starting leader node...${NC}"
    go run ./server/main.go -first_node=true -network_port=5000 -client_port=50021 > leader_log.txt 2>&1 &
    LEADER_PID=$!
    echo "Leader PID: $LEADER_PID"
    # Give it time to start
    sleep 3
    if ps -p $LEADER_PID > /dev/null; then
        echo -e "${GREEN}Leader node started successfully.${NC}"
    else
        echo -e "${RED}Leader node failed to start.${NC}"
        exit 1
    fi
}

# Start worker nodes
function start_workers() {
    echo -e "${YELLOW}Starting worker node 1...${NC}"
    go run ./server/main.go -network_port=5000 > worker1_log.txt 2>&1 &
    WORKER1_PID=$!
    echo "Worker 1 PID: $WORKER1_PID"
    
    sleep 3
    
    echo -e "${YELLOW}Starting worker node 2...${NC}"
    go run ./server/main.go -network_port=5000 > worker2_log.txt 2>&1 &
    WORKER2_PID=$!
    echo "Worker 2 PID: $WORKER2_PID"
    
    sleep 3
    
    # Check if workers are running
    if ps -p $WORKER1_PID > /dev/null && ps -p $WORKER2_PID > /dev/null; then
        echo -e "${GREEN}Worker nodes started successfully.${NC}"
    else
        echo -e "${RED}One or more worker nodes failed to start.${NC}"
        exit 1
    fi
}

# Test basic task submission
function test_basic_tasks() {
    echo -e "${YELLOW}Testing basic task submission...${NC}"
    
    # Submit a high priority multiplication task
    echo -e "${YELLOW}Submitting high priority multiplication task...${NC}"
    echo -e "1\n0\nHigh\n10\n0\n" | go run ./client/main.go -port=50021 > task1_output.txt 2>&1
    TASK1_ID=$(grep -o "got ID: [0-9]*" task1_output.txt | cut -d' ' -f3)
    
    # Submit a medium priority addition task
    echo -e "${YELLOW}Submitting medium priority addition task...${NC}"
    echo -e "1\n1\nMedium\n20\n0\n" | go run ./client/main.go -port=50021 > task2_output.txt 2>&1
    TASK2_ID=$(grep -o "got ID: [0-9]*" task2_output.txt | cut -d' ' -f3)
    
    # Submit a low priority fibonacci task
    echo -e "${YELLOW}Submitting low priority fibonacci task...${NC}"
    echo -e "1\n2\nLow\n15\n0\n" | go run ./client/main.go -port=50021 > task3_output.txt 2>&1
    TASK3_ID=$(grep -o "got ID: [0-9]*" task3_output.txt | cut -d' ' -f3)
    
    # Wait for tasks to complete
    echo -e "${YELLOW}Waiting for tasks to complete...${NC}"
    sleep 10
    
    # Check task status
    echo -e "${YELLOW}Checking task status...${NC}"
    
    if [ -n "$TASK1_ID" ]; then
        echo -e "2\n$TASK1_ID" | go run ./client/main.go -port=50021 > task1_status.txt 2>&1
        COMPLETED1=$(grep -c "is not completed" task1_status.txt)
        if [ "$COMPLETED1" -eq 0 ]; then
            echo -e "${GREEN}Task 1 (ID: $TASK1_ID) completed successfully.${NC}"
        else
            echo -e "${RED}Task 1 (ID: $TASK1_ID) is not completed yet.${NC}"
        fi
    fi
    
    if [ -n "$TASK2_ID" ]; then
        echo -e "2\n$TASK2_ID" | go run ./client/main.go -port=50021 > task2_status.txt 2>&1
        COMPLETED2=$(grep -c "is not completed" task2_status.txt)
        if [ "$COMPLETED2" -eq 0 ]; then
            echo -e "${GREEN}Task 2 (ID: $TASK2_ID) completed successfully.${NC}"
        else
            echo -e "${RED}Task 2 (ID: $TASK2_ID) is not completed yet.${NC}"
        fi
    fi
    
    if [ -n "$TASK3_ID" ]; then
        echo -e "2\n$TASK3_ID" | go run ./client/main.go -port=50021 > task3_status.txt 2>&1
        COMPLETED3=$(grep -c "is not completed" task3_status.txt)
        if [ "$COMPLETED3" -eq 0 ]; then
            echo -e "${GREEN}Task 3 (ID: $TASK3_ID) completed successfully.${NC}"
        else
            echo -e "${RED}Task 3 (ID: $TASK3_ID) is not completed yet.${NC}"
        fi
    fi
}

# Test task dependencies
function test_dependencies() {
    echo -e "${YELLOW}Testing task dependencies...${NC}"
    
    # Submit first task
    echo -e "${YELLOW}Submitting first task...${NC}"
    echo -e "1\n0\nHigh\n5\n0\n" | go run ./client/main.go -port=50021 > dep_task1_output.txt 2>&1
    DEP_TASK1_ID=$(grep -o "got ID: [0-9]*" dep_task1_output.txt | cut -d' ' -f3)
    
    # Submit dependent task
    echo -e "${YELLOW}Submitting dependent task...${NC}"
    echo -e "1\n1\nHigh\n10\n1\n$DEP_TASK1_ID\n" | go run ./client/main.go -port=50021 > dep_task2_output.txt 2>&1
    DEP_TASK2_ID=$(grep -o "got ID: [0-9]*" dep_task2_output.txt | cut -d' ' -f3)
    
    # Wait for tasks to complete
    echo -e "${YELLOW}Waiting for tasks to complete...${NC}"
    sleep 15
    
    # Check dependent task status
    echo -e "${YELLOW}Checking dependent task status...${NC}"
    echo -e "2\n$DEP_TASK2_ID" | go run ./client/main.go -port=50021 > dep_task2_status.txt 2>&1
    DEP_COMPLETED=$(grep -c "is not completed" dep_task2_status.txt)
    
    if [ "$DEP_COMPLETED" -eq 0 ]; then
        echo -e "${GREEN}Dependent task (ID: $DEP_TASK2_ID) completed successfully after parent task.${NC}"
    else
        echo -e "${RED}Dependent task (ID: $DEP_TASK2_ID) is not completed yet.${NC}"
    fi
}

# Test worker failure
function test_worker_failure() {
    echo -e "${YELLOW}Testing worker failure recovery...${NC}"
    
    # Submit a long task
    echo -e "${YELLOW}Submitting a task that will take time to complete...${NC}"
    echo -e "1\n2\nMedium\n30\n0\n" | go run ./client/main.go -port=50021 > failure_task_output.txt 2>&1
    FAILURE_TASK_ID=$(grep -o "got ID: [0-9]*" failure_task_output.txt | cut -d' ' -f3)
    
    # Wait a bit for task to be assigned
    sleep 5
    
    # Kill one worker
    echo -e "${YELLOW}Killing worker 1 to simulate failure...${NC}"
    kill -9 $WORKER1_PID
    echo -e "${RED}Worker 1 terminated.${NC}"
    
    # Wait for task reassignment and completion
    echo -e "${YELLOW}Waiting for task to be reassigned and completed...${NC}"
    sleep 20
    
    # Check task status
    echo -e "${YELLOW}Checking if task was completed despite worker failure...${NC}"
    echo -e "2\n$FAILURE_TASK_ID" | go run ./client/main.go -port=50021 > failure_task_status.txt 2>&1
    FAILURE_COMPLETED=$(grep -c "is not completed" failure_task_status.txt)
    
    if [ "$FAILURE_COMPLETED" -eq 0 ]; then
        echo -e "${GREEN}Task (ID: $FAILURE_TASK_ID) completed successfully despite worker failure.${NC}"
    else
        echo -e "${RED}Task (ID: $FAILURE_TASK_ID) is not completed yet.${NC}"
    fi
    
    # Restart the worker
    echo -e "${YELLOW}Restarting worker 1...${NC}"
    go run ./server/main.go -network_port=5000 > worker1_restart_log.txt 2>&1 &
    WORKER1_RESTART_PID=$!
    echo "Restarted Worker 1 PID: $WORKER1_RESTART_PID"
    sleep 5
}

# Test leader failure
function test_leader_failure() {
    echo -e "${YELLOW}Testing leader failure recovery...${NC}"
    
    # Submit some tasks before killing the leader
    echo -e "${YELLOW}Submitting tasks before leader failure...${NC}"
    echo -e "1\n0\nHigh\n8\n0\n" | go run ./client/main.go -port=50021 > leader_failure_task1.txt 2>&1
    LEADER_TASK1_ID=$(grep -o "got ID: [0-9]*" leader_failure_task1.txt | cut -d' ' -f3)
    
    # Kill the leader
    echo -e "${YELLOW}Killing leader to simulate failure...${NC}"
    kill -9 $LEADER_PID
    echo -e "${RED}Leader terminated.${NC}"
    
    # Wait for new leader election
    echo -e "${YELLOW}Waiting for new leader election...${NC}"
    sleep 20
    
    # Try to submit a new task to see if system recovered
    echo -e "${YELLOW}Attempting to submit a new task after leader recovery...${NC}"
    echo -e "1\n1\nHigh\n12\n0\n" | go run ./client/main.go -port=50021 > leader_recovery_task.txt 2>&1
    RECOVERY_TASK_ID=$(grep -o "got ID: [0-9]*" leader_recovery_task.txt | cut -d' ' -f3)
    
    if [ -n "$RECOVERY_TASK_ID" ]; then
        echo -e "${GREEN}Successfully submitted a new task (ID: $RECOVERY_TASK_ID) after leader recovery.${NC}"
        
        # Wait for task to complete
        sleep 10
        
        # Check task status
        echo -e "${YELLOW}Checking if task completed after leader recovery...${NC}"
        echo -e "2\n$RECOVERY_TASK_ID" | go run ./client/main.go -port=50021 > recovery_task_status.txt 2>&1
        RECOVERY_COMPLETED=$(grep -c "is not completed" recovery_task_status.txt)
        
        if [ "$RECOVERY_COMPLETED" -eq 0 ]; then
            echo -e "${GREEN}Task (ID: $RECOVERY_TASK_ID) completed successfully after leader recovery.${NC}"
        else
            echo -e "${RED}Task (ID: $RECOVERY_TASK_ID) is not completed yet.${NC}"
        fi
    else
        echo -e "${RED}Failed to submit a new task after leader failure. Leader recovery may have failed.${NC}"
    fi
}

# Cleanup
function cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    ./server_kill.sh 5001 5002 5003 50021
    echo -e "${GREEN}All processes terminated.${NC}"
}

# Run the tests
check_ports
start_leader
start_workers
test_basic_tasks
test_dependencies
test_worker_failure
test_leader_failure  # Uncomment this if leader failure test works well
cleanup

echo -e "${GREEN}==== Testing completed ====${NC}"