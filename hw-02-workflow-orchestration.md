tags:: homework
course:: [[de-zoomcamp]]

- ## Data Loader
- ```python
  import io
  import pandas as pd
  import requests
  if 'data_loader' not in globals():
      from mage_ai.data_preparation.decorators import data_loader
  if 'test' not in globals():
      from mage_ai.data_preparation.decorators import test
  
  @data_loader
  def load_data_from_api(*args, **kwargs):
      base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green'
  
      taxi_dtypes = {
          'VendorID': pd.Int64Dtype(),
          'passenger_count': pd.Int64Dtype(),
          'trip_distance': float,
          'RatecodeID': pd.Int64Dtype(),
          'store_and_fwd_flag': str,
          'PULocationID': pd.Int64Dtype(),
          'DOLocationID': pd.Int64Dtype(),
          'payment_type': pd.Int64Dtype(),
          'fare_amount': float,
          'extra': float,
          'mta_tax': float,
          'tip_amount': float,
          'tolls_amount': float,
          'improvement_surcharge': float,
          'total_amount': float,
          'congestion_surcharge': float 
      }
  
      parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
  
      months = ['10', '11', '12']
  
      dfs = []
  
      def fetch_data(base_url, months, taxi_dtypes, parse_dates):
          dfs = []
          for month in months:
              url = f"{base_url}/green_tripdata_2020-{month}.csv.gz"
              df = pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
              dfs.append(df)
          return pd.concat(dfs, ignore_index=True)
  
      return fetch_data(base_url, months, taxi_dtypes, parse_dates)
      # return pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
  
  
  @test
  def test_output(output, *args) -> None:
      assert output is not None, 'The output is undefined'
  
  ```
- ## Transformer
- ```python
  import pandas as pd
  
  if 'transformer' not in globals():
      from mage_ai.data_preparation.decorators import transformer
  if 'test' not in globals():
      from mage_ai.data_preparation.decorators import test
  
  
  @transformer
  def transform(data, *args, **kwargs):
      # create lpep_pickup_date
      data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
  
      # format column names
      data.columns = (data.columns
                      .str.replace('ID', '_id')
                      .str.lower()
      )
  
      return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
  
  
  @test
  def test_output(output, *args) -> None:
      assert output is not None, 'The output is undefined'
      assert 'vendor_id' in output.columns, 'Column "vendor_id" is not present in output'
      assert (output['trip_distance'] > 0).all(), 'Trip(s) with zero distance present in output'
      assert (output['passenger_count'] > 0).all(), 'Trip(s) with zero passengers present in output'
  
      unique_vendor_ids = output['vendor_id'].unique()
      print("Unique values in the 'vendor_id' column:", unique_vendor_ids)
  ```
- ## Data Exporter SQL (PostgreSQL, dev environment)
- ```python
  SELECT * FROM {{ df_1 }}
  ```
- ## Data Exporter parquet
- ```python
  import pyarrow as pa
  import pyarrow.parquet as pq
  import os
  
  if 'data_exporter' not in globals():
      from mage_ai.data_preparation.decorators import data_exporter
  
  
  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/de-zoomcamp-mage-1d916339386e.json"
  
  bucket_name = "de-zoomcamp-mage-bucket"
  project_id = "de-zoomcamp-mage"
  
  table_name = "nyc_taxi_data"
  
  root_path = f"{bucket_name}/{table_name}"
  
  @data_exporter
  def export_data(data, *args, **kwargs):
  
      table = pa.Table.from_pandas(data) # pyarrow needs a table
  
      gcs = pa.fs.GcsFileSystem() # authorizes using the GCS object
  
      pq.write_to_dataset(
          table,
          root_path=root_path,
          partition_cols=['lpep_pickup_date'],
          filesystem=gcs
      )
  ```