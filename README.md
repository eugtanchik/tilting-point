# Tilting Point Data Test

## Questions

1. Assume we are trying to collect data from a third party source via API calls. 
The data will have to be collected at least once per day. Design a solution.  
Solution. There could be several solutions of that problem. I would like to suggest an approach
based on Apache Airflow task scheduler. A directed acyclic graph (DAG) is designed for that
purpose. It includes task for API call and uploading data to AWS S3 storage. It runs on
daily schedule, and it is easy to monitor its logs using Airflow Web Server. For example, we can
get and store AdMob Mediation Reports data from AdMob API in such a way.

2. Based on question 1, assume you can successfully retrieve the data once per day, but sometimes 
the API returns incomplete data or no data. List possible reasons for this, as well as solutions 
to solving this.  
Possible reasons for incomplete or no data could be:
* Absence of the data for current date, or it is not available yet;
* API server is not working temporarily;
* Authentication problem may occur;
* Network connection problem on Client side;  
Apache Airflow task scheduler has tools to handle such kind of issues by means of automatic retrying of
failed tasks with a defined frequency and number of retries given. Therefore, an empty or incomplete data
would be ignored, and proper data would be collected into storage.

## Task Description

Assume we already have real time streaming data coming to our AWS S3, write some code to process 
the real time streaming data and have it REAL-TIME available in tables? 
(You can use Spark, Scala, Java, Python, etc. whatever the coding language you want)
Assume the input data source folder is: `s3://data/test`. Inside the `test` folder, it will have 
thousands of json files like: `0000.json`, `0001.json`, `0002.json` ... 
To simplify, example records JSON will look as follows:
```
{“user”: “A”, “timestamp”:”2017-12-19 10:41:49”, “spend”:”1.99”,”evtname”:”iap”}
{“user”: “B”, “timestamp”:”2017-12-20 11:22:18”, ”evtname”:”tutorial”}
{“user”: “A”, “timestamp”:”2017-12-20 18:20:10”, “spend”:”9.99”,”evtname”:”iap”}
```

## Output 

two tables (all data types are string)
```
table user
column: user, timestamp, evtname
```
```
table revenue
column: user, total_spend
```
