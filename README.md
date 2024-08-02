# football-etl-pipeline


## 1. Introduce

This project is an ETL (Extract, Transform, Load) pipeline designed to collect and process data related to top European football matches, including leagues like the English Premier League (EPL) and Serie A, La Liga,… This project culminates in a web application that allows users to search for and rank these matches.

## 2. Objective
Dataset is collected from [Kaggle](http://www.kaggle.com/datasets/technika148/football-database)

The primary objective of this project is to build an ETL pipeline that extracts data from various sources, transforms it into a structured format, and loads it into a database. This data is then used to power a web application that provides users with search capabilities and rankings of top football matches in Europe.

## 3. Design


### 3.1 Directory tree

### 3.2 Data pipeline
![image](https://github.com/user-attachments/assets/73c00c34-68c9-4af3-9f64-ae48706c2c20)

### 3.3 Database schema

### 3.4 Project structure

### 3.5 Data lineage
## 4. Setup


### 4.1 Git clone

### 4.2 Setup local

**#create docker**

make build

make up

### 4.3 Import data into MySQL

**#copy data from local to docker**

docker cp football/ de_mysql:/tmp/

**#enable access**

make to_mysql_root

SHOW GLOBAL VARIABLES LIKE 'LOCAL_INFILE';
SET GLOBAL LOCAL_INFILE=TRUE;

exit

**#create tables with schema**

make mysql_create

**#load csv into created tables**

make mysql_load

### 4.4 Create schema in PostgresSQL

**#create tables with schema**

make psql_create

## 5. Demo


## 6. Further actions


## 7. Conclusions
