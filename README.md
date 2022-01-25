# Weather Data Generation


Chuyu Chen  
January 25, 2022  

Get any 7 consecutive days of weather data for your city (or any US city of your choice), as well as weather from Detroit, Michigan for the same period. The script should have the ability to run for a date. Have it load to S3/Google Cloud Storage (GCS) location. Share that location and the github code repo.

__Requirements:__

1. Script should work when we git clone.
2. Script pulls weather data and date/time from public rest-api's for Detroit and the
additional city you chose. Load your results to S3/GCS.
3. S3/GCS is accessible to us, you decide how we get access.
4. Data in S3/GCS is formatted in a way that's easily loadable to a database table.
5. Commands to load data from S3/GCS to postgres or mysql or redshift table or Bigquery
and DDL for table.
6. Ideally the goal is for us to easily query :
select * from pubilc.weather_daily_table order by weather_date, location;
7. Optional not required bonus: Publish the data to Tableau public or similar location.


__Link to the GCS Location__
https://console.cloud.google.com/storage/browser/weather-data-cc

__Link to Google BigQuery__
https://console.cloud.google.com/bigquery?referrer=search&authuser=1&project=nimble-net-337716&supportedpurview=project&ws=!1m14!1m3!3m2!1snimble-net-337716!2spublic!1m4!4m3!1snimble-net-337716!2spublic!3sweather_daily_table!1m4!1m3!1snimble-net-337716!2sbquxjob_225c51ca_17e8ec9afa7!3sus-central1&page=dataset&d=public&p=nimble-net-337716

__DDl for the table loaded in Google BigQuery__

Google BigQuery has no primary key or unique constraints. Below is the DDL for Google BigQuery:
```
CREATE TABLE `nimble-net-337716.public.weather_daily_table`
(
  City STRING,
  State STRING,
  Country STRING,
  name STRING,
  datetime DATETIME,
  tempmax FLOAT64,
  tempmin FLOAT64,
  temp FLOAT64,
  feelslikemax FLOAT64,
  feelslikemin FLOAT64,
  feelslike FLOAT64,
  conditions STRING,
  description STRING
);
```

In general, with the primary key or unique constraints, the DDL would be as below:
```
```

__Publish the Data__

Please refer to this Kaggle location for the published dataset:
https://www.kaggle.com/chuyuchen/midwest-cities-weather-data-2021

The timespan has been changed to a year (2021), and the dataset covers the top 5 midwest cities including Chicago, Detroit, Indianapolis, Columbus, and Milwaukee.


__References:__

* https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
* https://googleapis.dev/python/storage/latest/index.html
* API: https://www.visualcrossing.com/weather-data
