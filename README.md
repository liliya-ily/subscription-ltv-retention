<h1 align="center">DS Docker Template</h1>

<p align="center">
  Reproducible environment for Data Science and ML projects with Jupyter, MLflow and VS Code Dev Containers.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white" />
  <img src="https://img.shields.io/badge/MLflow-3.15.1-0194E2?logo=mlflow&logoColor=white" />
</p>

## Overview

The template runs the project inside Docker and provides a ready-to-use Jupyter environment in VS Code together with a separate MLflow tracking server.

```mermaid
flowchart LR
    A[VS Code] --> B[Dev Container]
    B --> C[Jupyter / Python]
    C --> D[MLflow Tracking Server]
    D --> E[Experiments & Model Registry]
```

## Stack

* Python 3.12
* Jupyter
* pandas
* NumPy
* scikit-learn
* matplotlib
* MLflow
* Docker / Docker Compose
* VS Code Dev Containers

## Structure

```text
.
├── .devcontainer/
│   └── devcontainer.json
├── data/
├── notebooks/
├── src/
├── tests/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .dockerignore
├── .gitignore
├── README.md
└── setup.md
```

## How it works

The project directory is mounted into the development container as:

```text
/workspace
```

VS Code connects directly to this container through Dev Containers, so notebooks use the Python environment installed in Docker rather than a local Python installation.

MLflow runs as a separate Docker service.

From notebooks:

```text
http://mlflow:5000
```

From the host:

```text
http://localhost:5000
```

## Quick start

Open the repository in VS Code and run:

```text
Dev Containers: Reopen in Container
```

VS Code will start the required Docker services and open the project inside the development container.

Detailed setup and troubleshooting are in [setup.md](setup.md).

## Intended use

This repository is meant to be used as a starting point for separate DS/ML projects.

Project-specific libraries can be added to `requirements.txt`, while the Docker configuration keeps the environment reproducible.
