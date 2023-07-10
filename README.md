# Step 1:
run { docker compose up -d }
# Step 2: 
access { localhost:8080 } or { 0.0.0.0:8080 } to use the Airflow UI to monitor the pipeline
user: airflow
password: airflow
# Step 3:
run the "SQL Query" DAG

## Notice: I use airflow-providers service in order to connect to MySQL database. So check the connection setting before run the DAG ^^! Sorry my bad for not using simpler connector like sqlalchemy.
## Result: I got an error at "upload_to_db" task. It looks running without any Exception but there's still no data imported into SQL db (but db's size keeps increasing). Hope you guys help me find out ^^! And please check the "get_output" part, I didn't have data to test but I think it works! Anyway, NoSQL seems running well!
# Thank you!
