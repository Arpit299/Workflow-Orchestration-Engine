# Workflow Orchestration Engine

A Python-based workflow orchestration engine that executes DAG-based tasks with dependency management, parallel execution, retries, failure recovery, and execution reporting.

## Features

* DAG-based workflow execution
* Dependency validation
* Cycle detection
* Parallel task execution
* Worker pool
* Automatic retries
* Failure handling
* Dependency blocking
* Task execution history
* JSON workflow reports
* Custom workflow support
* Built-in demo workflow

## Tech Stack

**Python | Threading | ThreadPoolExecutor | JSON | CLI**

## DSA Used

**Dictionary | Set | defaultdict | deque | Graph | Topological Scheduling**

## Usage

Run the built-in demo:

```bash
python workflow_orchestration_engine.py --demo
```

Run without arguments:

```bash
python workflow_orchestration_engine.py
```

Generate a JSON report:

```bash
python workflow_orchestration_engine.py --demo --workers 3 --json workflow_report.json
```

Run a custom workflow:

```bash
python workflow_orchestration_engine.py workflow.json
```

## Architecture

```text
Workflow Definition
        ↓
Dependency Graph
        ↓
DAG Validation
        ↓
Ready Task Queue
        ↓
Worker Pool
        ↓
Parallel Execution
        ↓
Retry / Failure Recovery
        ↓
Dependency Resolution
        ↓
Execution Report
```

## Example

```text
extract
   ↓
validate ──────┐
   ↓           │
transform ─────┤
               ↓
             report
               ↓
            finalize
```

## Purpose

Built to demonstrate workflow orchestration, DAG algorithms, dependency scheduling, concurrency, retry strategies, and fault-tolerant task execution using Python.

