# Distributed Task Scheduling and Worker Coordination System

A distributed system for efficient task scheduling and worker coordination built with Go.

## Overview

This project implements a distributed task scheduling system using a master-slave architecture. The system efficiently distributes computational tasks across worker nodes, handles failures gracefully, and provides a priority-based scheduling mechanism.

Key features:
- Priority-based task scheduling
- Worker load balancing based on CPU utilization
- Leader election with Raft consensus algorithm
- Fault tolerance with heartbeat monitoring
- Task dependency management
- Dynamic worker node addition/removal

## Architecture

The system consists of:
- **Leader Node**: Manages the task queue and distributes tasks to workers
- **Worker Nodes**: Execute assigned tasks and report completion 
- **Client Interface**: Allows task submission and status checking

Communication between components is handled via gRPC for efficient and reliable message passing.

## Task Types

The system supports three types of computational tasks:
1. **Multiplication**: Factorial calculation
2. **Addition**: Sum calculation
3. **Fibonacci**: Fibonacci sequence calculation

Each task can have different priority levels (Low, Medium, High) and dependencies.

## Getting Started

### Prerequisites
- Go 1.15+
- Protocol Buffers compiler
- gRPC

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Karan14-11/Distributed_Project-.git
   cd Distributed_Project-
   ```

2. Install dependencies:
   ```bash
   go mod tidy
   ```

3. Generate protocol buffer code:
   ```bash
   make proto
   ```

### Running the System

1. Start the first node (leader):
   ```bash
   make first
   ```

2. Start additional worker nodes (in separate terminals):
   ```bash
   make server
   ```

3. Start a client to submit tasks:
   ```bash
   make client
   ```

## Testing

We've developed several testing tools to evaluate different aspects of the system. All test scripts are located in the project root directory.

### Basic System Test (simple_test.sh)

A comprehensive test script that verifies basic functionality and fault tolerance:

```bash
chmod +x simple_test.sh
./simple_test.sh
```

This test script:
- Starts a leader node and two worker nodes
- Tests basic task submission with different priorities
- Verifies task dependency functionality
- Tests worker failure recovery
- Tests leader failure recovery and election
- Provides colored output for easy verification

Example output:
```
==== Distributed Task Scheduling System - Basic Test Suite ====
Checking if test ports are in use...
Starting leader node...
Leader node started successfully.
Starting worker node 1...
Worker 1 PID: 12345
Starting worker node 2...
Worker 2 PID: 12346
Worker nodes started successfully.
Testing basic task submission...
Submitting high priority multiplication task...
Task 1 (ID: 1) completed successfully.
...
```

### Performance Testing (batch_testing.go)

Tests the system's performance with a specific number of concurrent clients:

```bash
go run batch_testing.go -clients=100
```

Parameters:
- `-clients`: Number of clients to simulate (default: 100)
- `-port`: Scheduler service port (default: 50021)
- `-output`: Custom output filename

This test:
- Simulates multiple clients submitting tasks concurrently
- Measures response and execution times for each task
- Calculates throughput and success rates
- Generates detailed CSV reports and summary statistics

### Load Balance Testing (lb_testing.go)

Evaluates how evenly tasks are distributed across worker nodes:

```bash
go run lb_testing.go -tasks=100
```

Parameters:
- `-tasks`: Number of tasks to submit (default: 100)
- `-port`: Scheduler service port (default: 50021)
- `-output`: Output file name (default: load_balance_report.txt)

This test will report:
- Worker task distribution
- Coefficient of variation (load balancing quality)
- Load imbalance percentage

Example output:
```
--- Load Balance Results ---
Worker 0: 33 tasks (33.0%)
Worker 1: 34 tasks (34.0%)
Worker 2: 33 tasks (33.0%)

--- Load Balancing Metrics ---
Tasks submitted: 100
Tasks completed: 100 (100.0%)
Worker nodes detected: 3
Average tasks per worker: 33.3
Standard deviation: 0.5
Coefficient of Variation: 1.5%
Load balancing quality: Excellent
Load imbalance: 3.0%
```

### System Load Testing (system_load_testing.go)

Tests how the system handles increasing load:

```bash
go run system_load_testing.go
```

This test:
- Incrementally increases the number of concurrent clients (10, 50, 100, 200, 300, 500)
- Measures success rate, response time, and throughput at each level
- Shows how the system scales with increased load

Example output:
```
Starting load test with increasing concurrent clients...
---------------------------------------------------
Concurrent Clients   Total Tasks     Success Rate    Avg Response Time    Throughput
---------------------------------------------------
10                   30              100.00%         0.50ms               12.71
50                   150             100.00%         0.27ms               41.53
100                  300             100.00%         0.72ms               72.05
200                  600             100.00%         0.98ms               94.89
300                  900             100.00%         0.83ms               117.96
500                  1500            100.00%         1.21ms               147.38
---------------------------------------------------
```

### Running All Tests (run_all_tests.sh)

To run comprehensive performance tests across multiple client loads:

```bash
chmod +x run_all_tests.sh
./run_all_tests.sh
```

This script:
1. Checks if your infrastructure is correctly running
2. Runs performance tests with different client counts (100, 500, 1000)
3. Stores results in the `test_results` directory

## System Performance

Our testing shows excellent performance characteristics:

| Concurrent Clients | Total Tasks | Success Rate | Avg Response Time | Throughput |
|--------------------|-------------|--------------|-------------------|------------|
| 10                 | 30          | 100.00%      | 0.50ms            | 12.71      |
| 50                 | 150         | 100.00%      | 0.27ms            | 41.53      |
| 100                | 300         | 100.00%      | 0.72ms            | 72.05      |
| 200                | 600         | 100.00%      | 0.98ms            | 94.89      |
| 300                | 900         | 100.00%      | 0.83ms            | 117.96     |
| 500                | 1500        | 100.00%      | 1.21ms            | 147.38     |

Key performance insights:
- **Perfect Success Rate**: Even at high load, all tasks complete successfully
- **Low Response Times**: Consistently under 1.5ms even with 500 concurrent clients
- **Linear Throughput Scaling**: Throughput scales almost linearly with client count
- **Excellent Load Balancing**: Tasks are evenly distributed across worker nodes

## Implementation Details

### Load Balancing
Tasks are distributed to worker nodes based on their current CPU load and active task count, ensuring efficient resource utilization.

### Leader Election
The system uses a simplified Raft consensus algorithm for leader election, ensuring there is always a single coordinator.

### Fault Recovery
- **Worker Failure**: Detected through heartbeat mechanism; tasks are reassigned
- **Leader Failure**: Triggers new leader election among remaining nodes

### Task Scheduling
Tasks are prioritized based on assigned priority level and dependencies. If two tasks have the same priority, they are processed in FIFO order.

## Project Structure

- `proto/`: Protocol buffer definitions
- `server/`: Leader and worker node implementation
- `client/`: Client implementation for task submission
  - `simple_test.sh`: Comprehensive system functionality test
  - `batch_testing.go`: Tests performance with specific client count
  - `lb_testing.go`: Tests load distribution across workers
  - `system_load_testing.go`: Tests scaling with increasing load
  - `run_all_tests.sh`: Runs comprehensive suite of tests

## Makefile Commands

- `make proto`: Generate protocol buffer code
- `make first`: Start the first node (leader)
- `make server`: Start a worker node
- `make client`: Start a client
- `make clean`: Clean generated files

## Team Members

- Mahika Jain (2021101096)
- Karan Nijhawan (2022101122)
- Harshit Gupta (2022101124)

