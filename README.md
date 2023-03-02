# Data-Engineer-Project4
Create data lake with Spark SQL

## Project Introduction
Sparkify in this project is a song stream music App. Currently the original datasets about songs information and users activities are stored in AWS S3 bucket in the form of JSON logs. This data engineer project aims to create an ETL process that reads the original data on S3 into designed tables. This will benefit the further queries for analyzing the data and generating insights on users activities. The generated tables will be stored as parquet files, which have better performance and use smaller storage.

## Data Modeling Process
According to the Star schema of relational database, 1 fast table and 4 dimension tables are designed from the original songs and logs datasets.
### Original datasets
songs dataset \
![song data](https://github.com/gyjbb/Data-Engineer-Project4/blob/main/song_data.png?raw=true)

logs dataset
![log data](https://github.com/gyjbb/Data-Engineer-Project4/blob/main/log_data.png?raw=true) 

### Fact table
**songplays**
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension tables
**songs**
song_id, title, artist_id, year, duration

**artists**
artist_id, name, location, latitude, longitude

**users**
user_id, first_name, last_name, gender, level

**time**
start_time, hour, day, week, month, year, weekday

## Run the project
This project will use Spark cluster to read datsets from the S3 bucket. \
Run the file etl.py to start an ETL process.

