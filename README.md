# luigi_queuing_example
Luigi Queuing Example

This project demonstrates a simple queuing system using [Luigi](https://github.com/spotify/luigi), a Python module that helps you build complex pipelines of batch jobs. It handles dependency resolution, workflow management, visualization, and more.

## Overview

The project consists of a Luigi pipeline that listens for new files in a specified directory (*trigger_data*). When a new file is detected, the pipeline processes the file and logs the results.

## Project Structure

- **main.py**: Contains the Luigi task definition and the logic to listen for new files.
- **logs/**: Directory where log files are stored.
- **trigger_data/**: Directory to monitor for new files.
- **Dockerfile**: Docker configuration for the Luigi scheduler and the application.
- **docker-compose.yml**: Docker Compose configuration to run the Luigi scheduler and the application.
- **requirements.txt**: Python dependencies.


## Running the Project

### Prerequisites

- Docker
- Docker Compose

### Steps

1. **Build and start the services**:
    ```sh
    docker-compose up --build
    ```

2. **Add a new file to the *trigger_data* directory**:
    ```sh
    echo "sample data" > trigger_data/new_file.txt
    ```

3. **Check the logs**:
    The logs will be generated in the *logs* directory, detailing the task's progress.

4. **Access the Luigi scheduler**:
    Open your browser and go to *http://localhost:8082* to see the Luigi scheduler interface.
