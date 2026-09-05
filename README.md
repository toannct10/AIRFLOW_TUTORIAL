# AIRFLOW_TUTORIAL
## Introduction

This repository is a hands-on learning project for Apache Airflow, an open-source platform for developing, scheduling, monitoring, and orchestrating data workflows.

The project focuses on understanding the core concepts and practical patterns used when building data engineering pipelines with Airflow. Each DAG in the dags/ directory demonstrates a specific Airflow feature, allowing the concepts to be learned progressively from simple workflows to data-aware orchestration patterns.

---

## Running with Docker Compose

The easiest way to run Apache Airflow for this tutorial is using **Docker Compose**. Docker Compose allows you to start the Airflow environment and its required services with a single command.

### 1. Start Airflow

From the project root directory, run:

```bash
docker compose up -d
```
The `-d` option runs the containers in the background.


### 2. Access the Airflow UI

Once the containers are running, open your browser and navigate to:

`http://localhost:8081`

Default credentials:

* **Username:** `airflow`
* **Password:** `airflow`

> The default credentials may be different if they have been changed in the Docker Compose configuration.

### 3. Stop Airflow

To stop the Airflow environment:

```bash
docker compose down
```

This stops and removes the containers created by Docker Compose while preserving the project files and mounted volumes.

## DAGs Overview

The dags/ folder contains a series of DAGs, with each DAG demonstrating a specific Airflow feature or workflow pattern.

* **`1_first_dag.py`**: Creating a basic Airflow DAG
* **`2_dag_versioning.py`**: Updating and versioning DAG definitions
* **`3_operators.py`**: Working with different types of Airflow operators
* **`4_XCOMs_auto.py`**: Using automatic XComs for task communication
* **`5_XCOMs_kwargs.py`**: Passing data between tasks using XComs and task context
* **`6_parallel_tasks.py`**: Executing independent tasks in parallel
* **`7_branches.py`**: Implementing branching and conditional task execution
* **`8_schedule_preset.py`**: Using predefined Airflow scheduling presets
* **`9_schedule_cron.py`**: Defining custom schedules with cron expressions
* **`10_schedule_delta.py`**: Scheduling DAGs using time intervals
* **`11_incremental_load.py`**: Implementing incremental data loading
* **`12_special_dates.py`**: Working with specific dates in Airflow scheduling
* **`14_asset_dependent.py`**: Defining DAG dependencies based on Assets
* **`asset_13.py`**: Creating a DAG using Airflow Assets
