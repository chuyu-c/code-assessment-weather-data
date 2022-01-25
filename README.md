# Weather Data Generation


Chuyu Chen  
January 25, 2022  

Get any 7 consecutive days of weather data for your city (or any US city of your choice), as well as weather from Detroit, Michigan for the same period. The script should have the ability to run for a date. Have it load to S3/Google Cloud Storage (GCS) location. Share that location and the github code repo.

#### Requirements:

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


#### Link to the [GCS Location](https://console.cloud.google.com/storage/browser/weather-data-cc): https://console.cloud.google.com/storage/browser/weather-data-cc

#### Link to [Google BigQuery](https://console.cloud.google.com/bigquery?referrer=search&authuser=1&project=nimble-net-337716&supportedpurview=project&ws=!1m14!1m3!3m2!1snimble-net-337716!2spublic!1m4!4m3!1snimble-net-337716!2spublic!3sweather_daily_table!1m4!1m3!1snimble-net-337716!2sbquxjob_225c51ca_17e8ec9afa7!3sus-central1&page=dataset&d=public&p=nimble-net-337716)

####


#### DDl for the table loaded in Google BigQuery

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

In general, with the primary key or unique constraints, the DDL would be as below (I was referencing to MySQL syntax):

```
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';


-- -----------------------------------------------------
-- Schema public
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `public` DEFAULT CHARACTER SET utf8 ;
USE `public` ;

-- -----------------------------------------------------
-- Table `public`.`weather_daily_table`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `public`.`weather_daily_table` (
  `datetime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `name` VARCHAR(45) NOT NULL,
  `City` VARCHAR(45) NULL DEFAULT NULL,
  `State` VARCHAR(45) NULL DEFAULT NULL,
  `Country` VARCHAR(45) NULL DEFAULT NULL,
  `tempmax` FLOAT(45) NULL DEFAULT NULL,
  `tempmin` FLOAT(45) NULL DEFAULT NULL,
  `temp` FLOAT(45) NULL DEFAULT NULL,
  `feelslikemax` FLOAT(45) NULL DEFAULT NULL,
  `feelslikemin` FLOAT(45) NULL DEFAULT NULL,
  `feelslike` FLOAT(45) NULL DEFAULT NULL,
  `conditions` VARCHAR(45) NULL DEFAULT NULL,
  `description` VARCHAR(45) NULL DEFAULT NULL,
  PRIMARY KEY (`timestamp`,`name`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;
```
* Please also reference to [DDL.sql](https://github.com/chuyu-c/weather-data-generation/blob/main/DDL.sql) for text file downloading.

#### Publish the Data

Please refer to [this Kaggle location](https://www.kaggle.com/chuyuchen/midwest-cities-weather-data-2021) for the published dataset.  
(https://www.kaggle.com/chuyuchen/midwest-cities-weather-data-2021)


* The timespan has been changed to a year (2021), and the dataset covers the top 5 midwest cities including Chicago, Detroit, Indianapolis, Columbus, and Milwaukee.


#### References:

* https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
* https://googleapis.dev/python/storage/latest/index.html
* API: https://www.visualcrossing.com/weather-data
