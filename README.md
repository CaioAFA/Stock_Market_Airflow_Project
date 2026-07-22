# Project Set Up
Run the following commands:
```bash
cd spark/master
docker build -t airflow/spark-master .

cd ../worker
docker build -t airflow/spark-worker .

cd ../..

cd spark/notebooks/stock_transform
docker build -t airflow/stock-app .

cd ../../..
astro dev start
```

You should see these containers running:
- webserver
- triggerer
- scheduler
- postgres
- minio
- metabase
- postgres-db

If somethng isn't working properly, access one of the containers (using Docker Desktop or /bin/bash command inside them) and execute:
```bash
airflow db upgrade
```

If something isn't right after restarting your environment:
```bash
astro dev stop

# Kill all containers
docker kill $(docker ps -q)

# Kill all process using the same ports
sudo kill -9 $(sudo lsof -t -i:2376)
sudo kill -9 $(sudo lsof -t -i:8082)
sudo kill -9 $(sudo lsof -t -i:9000)
sudo kill -9 $(sudo lsof -t -i:8081)
sudo kill -9 $(sudo lsof -t -i:8082)
sudo kill -9 $(sudo lsof -t -i:5432)
sudo kill -9 $(sudo lsof -t -i:7077)

docker context use default

astro dev start

# If it's not running, try to start manually via Docker Desktop
```

# Running / stopping
```bash
# Start containers
astro dev start

# Stop containers
astro dev stop
```


# Autocomplete on IDE
Execute these commands to make the code autocompletion available by installing the dependencies:
```bash
uv sync
```


# Setup Airflow connections
Take a look at the connections bellow and create on Airflow:
```
Connection: minio
# TODO: insert connection details here

```


Overview
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

Start Airflow on your local machine by running 'astro dev start'.

This command will spin up five Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- API Server: The Airflow component responsible for serving the Airflow UI and API
- Triggerer: The Airflow component responsible for triggering deferred tasks

When all five containers are ready the command will open the browser to the Airflow UI at http://localhost:8080/. You should also be able to access your Postgres Database at 'localhost:5432/postgres' with username 'postgres' and password 'postgres'.

Note: If you already have either of the above ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
