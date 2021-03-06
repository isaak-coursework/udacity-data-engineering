# Sparkify Airflow Data Pipelines

Sparkify wants to improve the maintainability and observability of their data pipelines by migrating their data warehouse population code to Apache Airflow!

This directory contains the code for the pipelines to continuously transfer Sparkify data from AWS S3 into their Redshift data warehouse. 

## Getting Started

### Spinning up Airflow 2.2.0

This repo uses **Airflow 2.2.0** running in Docker Compose. The code in this project **WILL NOT WORK WITH AIRFLOW VERSIONS < 2.0!!**

To start Airflow up, first make sure to `cd` to this directory.  
```bash
cd p5-airflow
```
Then run:
```bash
docker-compose build
docker-compose up airflow-init
```
This builds an image from the local Dockerfile, then initializes the Airflow DB and creates a default user. 

Finally, run this to start up airflow:
```bash
docker-compose up
```

This should make the Airflow UI available at <http://localhost:8080/>.

### Setting up AWS Redshift Resources

This project requires an active AWS Redshift cluster. Provision one on your AWS account, and attach an IAM role to the cluster that allows for S3 access. Make sure your Redshift instance is active and accessible via public internet. 

Before running the Airflow data pipelines, also make sure to execute all of the table definition statements in `create_tables.sql` against your Redshift cluster to ensure tables are available to be populated by Airflow DAGs. 

Create a copy of the example dwh.env configuration file: 
```bash
cp dwh.env.example dwh.env
```
Then fill out the details of the file with appropriate information about your Redshift Cluster (user, password, host, etc.). Make sure to save the new `dwh.env`. 

### Load Connections and Variables

Once Redshift is ready, ensure your local Airflow instance is still running and then run 
```
chmod u+x airflow.sh
chmod u+x load_connections.sh

./load_connections.sh
```

This will import the information you entered into the `dwh.env` file as appropriate Airflow Connections and Variables in the active Airflow instance. 

## Project Contents

### Files

- `dags/`
    * `udac_example_dag.py`
        * Main piece of code to run all operators and populate Sparkify's data warehouse from S3. 
- `plugins/`
    * `helpers/`
        * `sql_queries.py`
            * All select SQL statements to extract data from the Redshift staging tables. 
    * `operators/`
        * `stage_redshift.py`
            * Contains `StageToRedshiftOperator` for copying data from S3 into Redshift staging tables. 
        * `load_fact.py`
            * Contains `LoadFactOperator` for loading data from staging tables into Sparkify's fact table, `songplays`. 
        * `load_dimension.py`
            * Contains `LoadDimensionOperator` for loading data from staging tables into Sparkify's dimension tables. 
        * `data_quality.py`
            * Contains `DataQualityOperator` for running data quality checks to ensure the success of the other operators. 
    
### Data Model

This Airflow data pipeline creates a data lake with the below schema: 

![](warehouse_model.png)

In a production data pipeline, the LoadFactOperator would likely always append to the main fact table, and the runs would be partitioned by time, perhaps daily or even hourly. This would allow analysts and other database consumers to continuously consume log data produced by Sparkify customers. 