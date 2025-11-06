# Football Match Finder


This project is an ETL (Extract, Transform, Load) pipeline designed to collect and process data related to top European football matches, including leagues like the English Premier League (EPL) and Serie A, La Liga,… This project culminates in a web application that allows users to search for and rank these matches.

![image](https://github.com/user-attachments/assets/42cba9bf-5814-4e78-bf41-a2f0998f04c7)

## SYSTEM ARCHITECTURE

## Usage
- Docker desktop
- Dbeaver or any other DB client
- If using Windowns, install Linux on Windowns with WLS 

### 1 Git clone
Get clone repo: `get clone https://github.com/trungbac11/football-etl-pipeline.git`
### 2 Setup local

**#create docker**

`make build`

`make up`

### 3 Import data into MySQL

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

### 4 Create schema in PostgresSQL

**#create tables with schema**

`make psql_create`

### 5 Run ETL Pipeline with Dagster
Dagster will be running on: 'http://localhost:3001'

### 6 Run the Football Search Web Application

**#start the Streamlit app**

The web application will be available at: 'http://localhost:8501'

'make run_app'

### 7 Stop the Services

**#stop all containers**

'make down'
