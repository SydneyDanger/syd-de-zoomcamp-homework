## Preparing postgres and pgAdmin & ingesting data
- ```bash
  docker network create pg-network
  ```
- ```bash
  docker run -it ^
  -e POSTGRES_USER="root" ^
  -e POSTGRES_PASSWORD="root" ^
  -e POSTGRES_DB="homework-01" ^
  -v C:/Users/sydne/Documents/GitHub/syd-de-zoomcamp/homework/01-docker-terraform/postgres-data:/var/lib/postgresql/data ^
  -p 5432:5432 ^
  --network=pg-network ^
  --name=pg-database ^
  postgres:13
  ```
- ```bash
  docker run -it ^
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" ^
  -e PGADMIN_DEFAULT_PASSWORD="root" ^
  -p 8080:80 ^
  --network=pg-network ^
  --name=pgadmin ^
  dpage/pgadmin4
  ```
- ```sql
  SELECT COUNT(*)
  FROM yellow_taxi_data
  WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-09-18'
  AND CAST(lpep_dropoff_datetime AS DATE) = '2019-09-18';
  ```
- ```python
  from time import time
  import requests
  import pandas as pd
  import urllib.request
  from sqlalchemy import create_engine
  
  def get_file(url, output_name):
      if url.split('.')[-1] == 'gz':
          csv_name = output_name + ".csv.gz"
      else:
          csv_name = output_name + ".csv"
      
      response = requests.get(url)
      with open(csv_name, 'wb') as f:
          f.write(response.content)
          
  # create connection engine to postgres db
  engine = create_engine('postgresql://root:root@localhost:5432/homework-01')
  
  # load taxi lookup table
  get_file("https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv", "taxi_lookup")
  
  df_zones = pd.read_csv("taxi_lookup.csv")
  
  df_zones.to_sql(name='zones', con=engine, if_exists='replace')
  
  # load taxi data
  get_file("https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz", "taxi_data")
  
  df_data = pd.read_csv("taxi_data.csv.gz", low_memory=False)
  
  # check data types
  print(pd.io.sql.get_schema(df_data, name="yellow_taxi_data", con=engine))
  
  # convert data types
  df_data.lpep_pickup_datetime = pd.to_datetime(df_data.lpep_pickup_datetime)
  df_data.lpep_dropoff_datetime = pd.to_datetime(df_data.lpep_dropoff_datetime)
  
  # check data types again
  print(pd.io.sql.get_schema(df_data, name="yellow_taxi_data", con=engine))
  
  # load the taxi data in chunks
  df_iter = pd.read_csv('taxi_data.csv.gz', iterator=True, chunksize=100000)
  
  df = next(df_iter)
  # convert again
  df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
  df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
  
  # load first chunk
  df.head(n=0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace") # set replace for header
  df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append") # set append for each chunk
  
  # loop thru rest of chunks
  while True:
      t_start = time()
      df = next(df_iter)
      print("inserting %d rows" % df.shape[0])
  
      df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
      df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
  
      df.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")
  
      t_end = time()
  
      print("inserted %d rows in %.3f seconds" % (df.shape[0], (t_end - t_start)))
  print("all chunks inserted.")
  
  ```
- ## Question 3
	- ```sql
	  SELECT COUNT(*)
	  FROM yellow_taxi_data
	  WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-09-18'
	  AND CAST(lpep_dropoff_datetime AS DATE) = '2019-09-18';
	  ```
	- 15612
- ## Question 4
	- ```sql
	  SELECT *
	  FROM yellow_taxi_data
	  WHERE CAST(lpep_pickup_datetime AS DATE)
	  IN ('2019-09-18', '2019-09-16', '2019-09-26', '2019-09-21')
	  ORDER BY trip_distance DESC;
	  ```
	- 2019-09-26
- ## Question 5
	- ```sql
	  SELECT zpu."Borough", SUM(t.total_amount) AS total_amount_sum
	  FROM yellow_taxi_data t
	  JOIN zones zpu
	      ON t."PULocationID" = zpu."LocationID"
	  WHERE CAST(t.lpep_pickup_datetime AS DATE) = '2019-09-18'
	  AND zpu."Borough" <> 'Unknown'
	  GROUP BY zpu."Borough"
	  HAVING SUM(t.total_amount) > 50000
	  ORDER BY SUM(t.total_amount) DESC
	  LIMIT 3;
	  ```
	- Brooklyn, Manhattan, Queens
- ## Question 6
	- ```sql
	  SELECT t.index, t.tip_amount, zpu."Zone" as PUZone, zdo."Zone" as DOZone
	  FROM yellow_taxi_data t
	  JOIN zones zpu
	  	ON t."PULocationID" = zpu."LocationID"
	  JOIN zones zdo 
	  	ON t."DOLocationID" = zdo."LocationID"
	  WHERE CAST(t.lpep_pickup_datetime AS DATE) >= '2019-09-01'
	  AND CAST(t.lpep_pickup_datetime AS DATE) <= '2019-09-30'
	  AND zpu."Zone" = 'Astoria'
	  ORDER BY t.tip_amount DESC
	  LIMIT 1;
	  ```
	- JFK Airport
- ## Question 7
	- ```bash
	  google_storage_bucket.data-lake-bucket: Refreshing state... [id=terraform-demo-411718-demo-bucket]
	  
	  Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the  
	  following symbols:
	    + create
	  
	  Terraform will perform the following actions:
	  
	    # google_bigquery_dataset.dataset will be created
	    + resource "google_bigquery_dataset" "dataset" {
	        + creation_time              = (known after apply)
	        + dataset_id                 = "demo_dataset"
	        + delete_contents_on_destroy = false
	        + etag                       = (known after apply)
	        + id                         = (known after apply)
	        + labels                     = (known after apply)
	        + last_modified_time         = (known after apply)
	        + location                   = "US"
	        + project                    = "terraform-demo-411718"
	        + self_link                  = (known after apply)
	      }
	  
	  Plan: 1 to add, 0 to change, 0 to destroy.
	  
	  Do you want to perform these actions?
	    Terraform will perform the actions described above.
	    Only 'yes' will be accepted to approve.
	  
	    Enter a value: yes
	  
	  google_bigquery_dataset.dataset: Creating...
	  google_bigquery_dataset.dataset: Creation complete after 1s [id=projects/terraform-demo-411718/datasets/demo_dataset]
	  
	  Apply complete! Resources: 1 added, 0 changed, 0 destroyed.
	  ```