![image](https://github.com/user-attachments/assets/73c00c34-68c9-4af3-9f64-ae48706c2c20)

### 3. Set up

**#create docker**

make build

make up

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

**#create tables with schema**

make psql_create
