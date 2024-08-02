# football-etl-pipeline


This project is an ETL (Extract, Transform, Load) pipeline designed to collect and process data related to top European football matches, including leagues like the English Premier League (EPL) and Serie A, La Liga,… This project culminates in a web application that allows users to search for and rank these matches.

![image](https://github.com/user-attachments/assets/42cba9bf-5814-4e78-bf41-a2f0998f04c7)



## 1. Objective
Dataset is collected from [Kaggle](http://www.kaggle.com/datasets/technika148/football-database)

The primary objective of this project is to build an ETL pipeline that extracts data from various sources, transforms it into a structured format, and loads it into a database. This data is then used to power a web application that provides users with search capabilities and rankings of top football matches in Europe.
## 2. Design

### 2.1 Directory tree

### 2.2 Data pipeline

![image](https://github.com/user-attachments/assets/f853b927-af81-4863-832c-360f96c93f20)


### 2.3 Database schema

### 2.4 Project structure

### 2.5 Data lineage

## 3. Setup
- Docker desktop
- Dbeaver or any other DB client
- If using Windowns, install Linux on Windowns with WLS 

### 3.1 Git clone
Get clone repo: `get clone https://github.com/trungbac11/football-etl-pipeline.git`
### 3.2 Setup local

**#create docker**

`make build`

`make up`

### 3.3 Import data into MySQL

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

### 3.4 Create schema in PostgresSQL

**#create tables with schema**

`make psql_create`

## 4. Demo

## 5. Further actions

## 6. Conclusions
