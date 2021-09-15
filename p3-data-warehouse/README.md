# Sparkify AWS Data Warehouse

Sparkify wants to enhance their analytical capabilities by constructing a cloud data warehouse on AWS infrastructure! This repo contains the code for:
- Data model definition for Sparkify's new data warehouse.
- ETL code to move data from S3 into Redshift data warehouse.

## Data Model

The final data model for the warehouse is a star schema model.

Facts Table: **songplays** - Logged individual plays of songs by Sparkify users (1 row per play).

Dimension Tables: 
- **users** - Details about users, such as name and gender. 
- **songs** - Details about each song, such as title and length.
- **artists** - Recording artist info - name, location
- **time** - Convenience table with conversions of timestamps into days, weekdays, etc. 

*Insert Data Model ER Diagram*

### Data Model Performance Decisions
The facts table, **songplays**, is distributed by 