![football-etl-pipeline - Frame 1 (3)](https://github.com/user-attachments/assets/60813239-8065-46b0-8440-0db4ab58ef27)

## Database Schema

<img width="1591" height="1006" alt="image" src="https://github.com/user-attachments/assets/89f424a6-c8d3-4d3e-979f-b1cdacda82b7" />

## Data Lineage
![football-etl-pipeline - Frame 2 (1)](https://github.com/user-attachments/assets/fd959d75-8ab9-4b18-9fa3-14edf03c07ef)

#### Bronze layer 
<img width="1915" height="790" alt="image" src="https://github.com/user-attachments/assets/7edfe7a8-fb2b-4bc0-97cb-42647954fb79" />

#### Silver layer
<img width="1915" height="551" alt="image" src="https://github.com/user-attachments/assets/8442540e-9754-4c29-844d-50167dabd3e2" />

#### Gold layer
<img width="1908" height="593" alt="image" src="https://github.com/user-attachments/assets/5f1e4953-1cea-4e85-9679-67c2919c3971" />

#### Data Warehouse
<img width="1914" height="789" alt="image" src="https://github.com/user-attachments/assets/1fe8a763-d679-431d-a5a6-772ce6571042" />

## Demo
<img width="1920" height="1920" alt="BeautyPlus-Collage-2025-11-06T17_33_43" src="https://github.com/user-attachments/assets/61a2ae0c-2d8a-4ccb-9aee-2e1be10bdd14" />

## Usage
- Docker desktop
- Dbeaver or any other DB client
- If using Windowns, install Linux on Windowns with WLS 

### Git clone
Get clone repo: `get clone https://github.com/trungbac11/football-etl-pipeline.git`
### Setup local

**create docker**

`make build`

`make up`

### Import data into MySQL

**copy data from local to docker**

`docker cp football/ de_mysql:/tmp/`

**enable access**

`make to_mysql_root`

`SHOW GLOBAL VARIABLES LIKE 'LOCAL_INFILE';`

`SET GLOBAL LOCAL_INFILE=TRUE;`

`exit`

**create tables with schema**

`make mysql_create`

**load csv into created tables**

`make mysql_load`

### Create schema in PostgresSQL

**create tables with schema**

`make psql_create`

### Run ETL Pipeline with Dagster
Dagster will be running on: `http://localhost:3001`

### Demo

**start the Streamlit app**: `http://localhost:8501`
