## SYSTEM ARCHITECTURE

<img width="2000" height="965" alt="image" src="https://github.com/user-attachments/assets/a2c58a8c-c52d-40b6-840e-2acaad43e8a0" />

## Usage
- Docker desktop
- Dbeaver or any other DB client
- If using Windowns, install Linux on Windowns with WLS 

### Git clone
Get clone repo: `get clone https://github.com/trungbac11/football-etl-pipeline.git`
### Setup local

**#create docker**

`make build`

`make up`

### Import data into MySQL

**#copy data from local to docker**

`docker cp football/ de_mysql:/tmp/`

**#enable access**

`make to_mysql_root`

`SHOW GLOBAL VARIABLES LIKE 'LOCAL_INFILE';`

`SET GLOBAL LOCAL_INFILE=TRUE;`

`exit`

**#create tables with schema**

`make mysql_create`

**#load csv into created tables**

`make mysql_load`

### Create schema in PostgresSQL

**#create tables with schema**

`make psql_create`

### Run ETL Pipeline with Dagster
Dagster will be running on: `http://localhost:3001`

### Demo

**#start the Streamlit app**

The web application will be available at:  `http://localhost:8501`

`make run_app`
