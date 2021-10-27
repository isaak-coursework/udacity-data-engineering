# Data Engineering Capstone Project

## Project Intent

The purpose of this project is to create a database that can be used to analyze immigration and travel trends into the United States at both the city- and state-level of aggregation. 

### Example Driving Questions

- Are people more likely to visit or emigrate to the United States in warmer months, or to warmer cities/states in general?
- Are immigrants more likely to settle in states with higher foreign-born populations?
- Does number of airports in a city affect the flow of immigration/travel through the city? 

## Database Model

### Central Tables - US Immigration/Travel Data

**Base Table** - Source of Materialized Views

- `immigration_base` - This table is not meant to be queried directly. It is a staging table, on top of which I have created multiple aggregate views. These views can be joined to the supplementary dimensional tables on state- and city-level columns. This allows for easy analysis of trends around US cities and states related to immigration and travel. 

| Column | Type | Description |
| ------ | ---- | ----------- |
| immigrant_id | INTEGER | Unique identifier |
| country_of_citizenship_code | INTEGER | Identifier for country of citizenship at time of departure |
| country_of_citizenship | TEXT | Country of citizenship at time of departure |
| country_of_residence_code | INTEGER | Identifier for country of residence at time of departure |
| country_of_residence | TEXT | Country of residence at time of departure |
| arrival_port_code | TEXT | Identifier for arrival location |
| arrival_port | TEXT | Arrival location |
| arrival_port_city | TEXT | City of arrival |
| arrival_port_state | TEXT | State of arrival |
| arrival_date | TIMESTAMP | Arrival date |
| arrival_mode_code | INTEGER | Identifier for method of arrival |
| arrival_mode | TEXT | Method of arrival |
| departure_date | TIMESTAMP | Date of departure |
| departure_month | INTEGER | Month of departure - 1-12 |
| airline | TEXT | Airline for arrival, if flew |
| flight_number | TEXT | Flight number |
| state_settled_code | TEXT | 2-digit identifier for state settled |
| state_settled | TEXT | Name of state settled |
| age | INTEGER | Age |
| birth_year | INTEGER | Birth Year |
| visa_code | INTEGER | Identifier for Visa reason |
| visa_reason | TEXT | Reason for travel Visa |
| visa_type | TEXT | Type of visa |
| gender | TEXT | Gender |

<br/>
 
**Immigration Table Materialized Views**

- `immigrant_stay_length_by_state`

| Column | Type | Description |
| ------ | ---- | ----------- |
| state_settled_code | TEXT | 2-digit identifier for state settled |
| departure_month | INTEGER | Month of departure - 1-12 |
| no_months_in_usa | INTEGER | Number of months spent in USA |
| no_immigrants | INTEGER | Total count of immigrants |

<br/>

- `immigrants_by_city`

| Column | Type | Description |
| ------ | ---- | ----------- |
| arrival_port_state | TEXT | 2-digit identifier for arrival state |
| arrival_port_city | TEXT | Name of arrival city |
| no_immigrants | INTEGER | Total count of immigrants |
| avg_age | INTEGER | Average Age |
| no_males | INTEGER | Count of Males |
| no_females | INTEGER | Count of Females |

<br/>

- `immigrants_by_state`

| Column | Type | Description |
| ------ | ---- | ----------- |
| arrival_port_state | TEXT | 2-digit identifier for arrival state |
| no_immigrants | INTEGER | Total count of immigrants |
| avg_age | INTEGER | Average Age |
| no_males | INTEGER | Count of Males |
| no_females | INTEGER | Count of Females |

<br/>

- `immigrants_by_state_settled`

| Column | Type | Description |
| ------ | ---- | ----------- |
| state_settled_code | TEXT | 2-digit identifier for state settled |
| no_immigrants | INTEGER | Total count of immigrants |
| avg_age | INTEGER | Average Age |
| no_males | INTEGER | Count of Males |
| no_females | INTEGER | Count of Females |

*More materialized views could easily be created by analysts - this is why the base table remains preserved and available.*

### Dimensional Tables - Supplementary Information

- `temperatures` - Average US temperatures by state and city, per month. 

| Column | Type | Description |
| ------ | ---- | ----------- |
| state_code | TEXT | 2-digit identifier for US state |
| city | TEXT | US city name |
| month | INTEGER | Month of departure - 1-12 |
| avg_temperature | NUMERIC | Average Temperature |
| avg_temperature_uncertainty | NUMERIC | Average Temperature Uncertainty |

<br/>

- `demographics` - Demographic statistics by US city and state. 

| Column | Type | Description |
| ------ | ---- | ----------- |
| city | TEXT | US city name |
| state | TEXT | US state name |
| median_age | NUMERIC | Median Age |
| male_population | NUMERIC | Male Population |
| female_population | NUMERIC | Female Population |
| total_population | INTEGER | Total Population |
| no_veterans | NUMERIC | Population of US Veterans |
| is_foreign_born | NUMERIC | Is foreign born or not |
| avg_household_size | NUMERIC | Average Household Size |
| state_code | TEXT | 2-digit identifier for US state |
| race | TEXT | Race |
| count | INTEGER | Count |

<br/>

- `airports` - Airport statistics by US city and state. 

| Column | Type | Description |
| ------ | ---- | ----------- |
| state_code | TEXT | 2-digit identifier for US state |
| city | TEXT | US city name |
| no_airports | INTEGER | Number of airports |
| avg_elevation | NUMERIC | Average elevation of airports |
| most_common_type | TEXT | Most common type of airport - null if tied |

<br/>

## Technologies Used

I am a firm believer in not prematurely over-complicating tasks, especially engineering tasks. This project therefore is based mainly on using Python's Pandas library to move and manipulate data, and the database model is ultimately loaded into a PostGreSQL database. Pandas proved to be plenty fast and capable for the data involved in this project, and queries on the final schema run very responsively on even a local instance of PostGres. 

For ease of development and testing, this project is set up to spin up a local PostGreSQL DB in Docker, and then load data into that database using Python scripts, with the main entrypoint being `load.py`. 

I wrote a wrapper CLI for interacting with the PostGreSQL, `fake-redshift.sh`, to ease the running of the project. The name is a nod to the fact that PostGreSQL, while convenient for testing, should be replaced with a Redshift cluster were this project to turn into a live production system. 

For reproducibility, all data has been loaded to an exposed S3 bucket located at `s3://p6-capstone/data` on AWS us-east-1. 

This project also interacts briefly with the Google Geocoding API for enriching the global temperatures dataset. The results of that enrichment have been included in the above S3 bucket, so that reproducing this project does not require making a Google account. 

## Other Scenarios

- The data was increased by 100x.
    * As mentioned above, this project should definitely be shifted to a live Redshift Cluster were it to be scaled and run in a live, production system. 
    * At that scale of data, Pandas would run out of memory. I would shift to using PySpark scripts for performing data manipulation. 
        * If only the main immigration data source were to scale (since some other sources are operating at higher levels of aggregation), I would only shift the immigration data to PySpark. It is always better to leave anything possible at a lower level of complexity. 
        * I would deploy my PySpark jobs on [AWS Glue](https://aws.amazon.com/glue/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc). This is a managed PySpark ETL service. As I am a one person team, managing an EMR cluster might be a tall order. 
- The pipelines would be run on a daily basis by 7 am every day.
    * This pipeline could be transitioned to an Apache Airflow DAG. The project is already set up with this in mind. 
    * DAG tasks:
        1. Create tables (if they do not exist). 
        2. Run ETL processes, appending new records. 
        3. Refresh materialzed views on top of `immigration_base` table. 
        4. Run data quality checks. 
- The database needed to be accessed by 100+ people.
    * Switching to a Redshift cluster would help dramatically with scaling to this many users. 

## Running the Project Locally

First, clone this repo and cd to the Capstone Project directory: 

```bash
git clone https://github.com/isaak-coursework/udacity-data-engineering.git
cd udacity-data-engineering/p6-capstone
```

The entrypoint script for our local PostGreSQL DB is `fake-redshift.sql`. First, ensure you can run the script, then pull the docker image for PostGreSQL: 

```bash
chmod u+x fake-redshift.sh
./fake-redshift.sh get-image
```

Then run the container to create a PostGres instance, as well as create a DB called `dev`. 

```bash
./fake-redshift.sh create
```

Next, create our initial DB schema by running the contents of `sql/model.sql`: 

```bash
./fake-redshift.sh execute "model.sql"
```

Before running the main script, you will need to create a `.env` file with the necessary credentials for running the scripts. So run:

```bash
cp .env.example .env
```

Then populate the `.env` fields with valid AWS CLI credentials. This will allow you to pull the necessary data from my S3 account, where I have made it public at `s3://p6-capstone/data/`. 

**Note**: You can leave the `GOOGLE_API_KEY`, the script will by default use pre-enriched data that I have fetched using Google's API and stored in S3. 

Now time to populate our DB! Install necessary python dependencies, then run the pipeline scripts:

```bash
pip install -r requirements.txt
python load.py
```

This may take as long as an hour to run, but hopefully ~30min. 

Finally, run the SQL statements to create the materialized views on top of the `immigration_base` table, allowing for easier joins to other tables. 

```bash
./fake-redshift.sh execute mviews.sql
```

## Example Queries

- Are immigrants more likely to settle in states with higher foreign-born populations?

```sql
SELECT 
    immi.state_settled_code,
    immi.no_immigrants,
    demo.is_foreign_born
FROM immigrants_by_state_settled immi
INNER JOIN demographics demo
    ON immi.state_settled_code = demo.state_code;
```

- Does number of airports in a city affect the flow of immigration/travel through the city? 

```sql
SELECT 
    immi.arrival_port_state,
    immi.arrival_port_city,
    immi.no_immigrants,
    airp.no_airports
FROM immigrants_by_city immi
INNER JOIN airports airp
    ON immi.arrival_port_state = airp.state_code
    AND immi.arrival_port_city = airp.city;
```
