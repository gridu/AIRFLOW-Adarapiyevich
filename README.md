# Airflow Capstone Project

Consists of 3 folders:
* _dags_ contains definitions for pipelines
* _docker_ contains files required for creating docker images and running Airflow cluster
* _plugins_ contains python module with the custom operator

## Run a cluster

In order to run a cluster `docker-compose -f docker/docker-compose.yml up` command should be executed.
####Points to consider
* mount paths are machine dependent
* upon the first run _initdb_ service should be uncommented
* upon the first run _postgres_ service needs some time for initialization. Therefore, it should be run separately.


## Trigger DAGs

DAGs can be triggered via Web UI(unpause & trigger DAG) hosted on http://localhost:8080 or by executing Airflow CLI commands inside docker container
```bash
$ docker exec -it docker_scheduler_1 /bin/bash
$ airflow unpause process_second_table
$ airflow trigger_dag process_second_table
```

*trigger_table_update* DAG contains _Sensor_ task which waits for a file to be created under path stored in _Variable_ *trigger_file*

## Plugin
Since _plugins_ folder is mounted into containers' $AIRFLOW_HOME/plugins directory, custom operator will be discovered by Airflow at bootstrap time and no extra-actions are required