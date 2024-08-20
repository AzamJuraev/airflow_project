# Airflow Docker Deployment

#### airflow-webserver: The Airflow webserver, which provides the web-based UI for managing and monitoring workflows.

#### airflow-scheduler: The Airflow scheduler, which is responsible for executing tasks according to the defined DAG schedule.

#### airflow-worker: The Airflow worker, which executes tasks on behalf of the scheduler.

#### airflow-init: A service that initializes the Airflow metadata database and creates an initial user account.

#### postgres: The PostgreSQL database that Airflow uses to store metadata.


## RUN commands
docker compose up airflow-init  
Initialize databse 


docker compose --env-file (dir for env) up -d    

## RESTART Container

docker compose restart *container*

## STOP
docker compose stop *container*

# airflow_project
