This is a test environment for docker compose, dbt, and airflow.

# Prerequisites
* 4GB RAM
* 2 CPU kernels

# Initializing Environment
Everything described is only needs for the first time.

## avoid sudo
If you have to use `sudo` for your docker commands, you should  open a shell and run:
```bash
sudo usermod -aG docker $USER
```
And then logout then login again.
Now you can run docker commands without `sudo`.
This also seems to be neeeded to run docker compose.

## initialize airflow environment
On Linux, open a shell and run:
```bash
AIRFLOW_DIRECTORY=/var/airflow

sudo mkdir $AIRFLOW_DIRECTORY
sudo chown $(id -u):$(id -g) $AIRFLOW_DIRECTORY

mkdir $AIRFLOW_DIRECTORY/dags
mkdir $AIRFLOW_DIRECTORY/logs
mkdir $AIRFLOW_DIRECTORY/plugins

cd local-attempt
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

If you are running airflow `2.0.1`, run:
```bash
docker build -f ./Dockerfile.airflow2.0.1.fix -t apache/airflow:2.0.1 .
```
See https://github.com/apache/airflow/issues/14266#issuecomment-796923376

Then, run:
```bash
docker-compose up airflow-init
```

Based on https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html


# Use Environment

## start airflow environment
On Linux, open a shell and run:
```bash
docker-compose up
```
