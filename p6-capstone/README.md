# Data Engineering Capstone Project

## AWS Glue - Local Development Environment

This folder contains resources for running an AWS Glue 2.0 development environment locally in Docker. 

All commands run through the `glue-local.sh` script, such as building the Docker image, starting a PySpark shell, and running a Glue PySpark script. 

The Dockerfile is based on the instructions from: <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html>

## Getting Started

Build the image: 

```bash
./glue-local.sh build
```

### Options

Start up a pyspark shell with Glue libraries loaded: 

```bash
./glue-local.sh pyspark
```

Submit a script to Glue PySpark for execution (must be in `jobs/` directory)

```bash
./glue-local.sh glue-submit <script_name.py>
```

Open up a bash shell inside the running Docker Container

```bash
./glue-local.sh bash
```
