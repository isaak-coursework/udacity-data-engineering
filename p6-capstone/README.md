# Data Engineering Capstone Project

## Project Intent

## Technologies Used



## Other Scenarios

- The data was increased by 100x.
- The pipelines would be run on a daily basis by 7 am every day.
- The database needed to be accessed by 100+ people.

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
