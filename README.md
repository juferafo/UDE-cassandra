# UDE-cassandra

The goal of this repository is to analyze activity data that is similar to the information present in the logs generated in well known streming apps. Such information includes speciphic user data like, for example, user location, number of songs listened or how often the user listens to music. These details are crucial for companies that offer streaming services since, based on its analysis, it is possible to get insights that relate to user behavior and make business decissions. 

In order to achieve this goal it is necessary to implement a "playground" that makes possible the analysis of the data. The design proposed here employes the open-source software [Apache Cassandra](https://cassandra.apache.org/) to create a [NoSQL](https://www.xenonstack.com/blog/nosql-databases/) database where to store and manage data. As one can imagine, there are multiple ways to achieve this goal. Depending on the size of the data we could consider the benefits of using a SQL database. The work presented here can be thought as a continuation/evolution of the use-case presented in [this repository](https://github.com/juferafo/UDE-postgres).

### Activity data

The raw data can be found in the directory `./event_data` that contains CSV files with user activity:

```
./event_data
├── 2018-11-01-events.csv
├── 2018-11-02-events.csv
├── 2018-11-03-events.csv
├── 2018-11-04-events.csv
├── 2018-11-05-events.csv
...
```

These files are partitioned by date with the format `YYYY-MM-DD-events.csv`. The streaming applications can record multiple details of the user activity but, for sake of simplicity, only the below fields are present: 

```
{
  artist TEXT,
  auth TEXT,
  firstName TEXT,
  gender TEXT,
  itemInSession INT,
  lastName TEXT,
  length DOUBLE,
  level TEXT,
  location TEXT,
  method TEXT,
  page TEXT,
  registration DOUBLE,
  sessionId INT,
  song TEXT,
  status INT,
  ts FLOAT,
  userId INT
}
```

In order to process all this information we will make use of an ETL (extract, transform, load) pipeline that brings the CSV data all-together into a single file. Due to the nature of this use-case it is expected that in a real scenario for widely used applications terabytes of data are generated. In these situations it is more convenient to employ a NoSQL model instead the regular SQL ones. We will use [Apache Cassandra](https://github.com/apache/cassandra) to host the database since it is a highly-scalable partitioned row store. This means that the data is organized in rows and columns (like in the SQL model) and partitioned by an unique key/identifier.  

In Apache Cassandra the queries are executed in the Cassandra Query Language [CQL](https://cassandra.apache.org/doc/latest/cql/) which is very similar to SQL. However, it is very important to have in mind that JOIN and GOUP BY statements do not exist in Apache Cassandra. This has two implications: first the data must undergo a process of [denormalization](https://www.datastax.com/blog/basic-rules-cassandra-data-modeling) and secondly the database must be modelled according to the questions that we want to answer or, in other words, the target queries. Below you can find the target queries that will be employed to model the database.

##### Query 1: What is the artist, song title and song's length in the music app history that was heard during sessionId = 338 and itemInSession = 4?

CQL of the query 1: `SELECT artist, song, length FROM <table_name> WHERE sessionId = 228 AND itemInSession = 4`

##### Query 2: What is the name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182?

CQL of the query 2: `SELECT artist, song, firstName, lastName FROM <table_name> WHERE userid = 10 AND sessionid = 182`

##### Query 3: What are the user names (first and last) in the music app history who listened to the song 'All Hands Against His Own'?

CQL of the query 3: `SELECT firstName, lastName FROM <table_name> WHERE song = 'All Hands Against His Own'`

### Code workflow

The workflow employed to set-up this use-case can be found in the Jupyter notebook `./Project_1B_ Project_Template.ipynb` and it is divided into two steps: (1) a data preprocessing stage and (2) the database configuration followed by data ingestion.

#### Step 1: data preprocessing

This step is included in the Part I of `./Project_1B_ Project_Template.ipynb` its purpose is to gather from the event data files the required information to answer the target queries. For this purpose, the code follwing code is employed:

```
[...]

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            # The rows without artist information (empty strings) are skipped
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

[...]
```

As one can notice, the processing logic is implemented to walk through the event logs and gather the values in the fields `artist`, `firstName`, `gender`, `itemInSession`, `lastName`, `length`, `level`, `location`, `sessionId`, `song`, `userId`. All this information will be written into a single file named `./event_datafile_new.csv` with the following schema:

```
{
  artist TEXT,
  firstName TEXT,
  gender TEXT,
  itemInSession INT,
  lastName TEXT,
  length DOUBLE,
  level TEXT,
  location TEXT,
  sessionId INT,
  song TEXT,
  userId INT
}
```

#### Step 2: database configuration and data ingestion 

This step is included in the Part II of `./Project_1B_ Project_Template.ipynb`. Once the valuable data has been saved into `./Project_1B_ Project_Template.ipynb` it is time to configure the database. The first part of this step is to generate the [keyspace](https://cassandra.apache.org/doc/latest/cql/ddl.html#create-keyspace) that will host the tables. For this purpose we have employed the below DDL statement

```
CREATE KEYSPACE IF NOT EXISTS sparkify
    WITH REPLICATION = {'class' : 'SimpleStrategy',
                       'replication_factor' : 1}"
```

The `REPLICATION` parameters are employed to configure how the data is [distributed accross the cluster](https://docs.datastax.com/en/cassandra-oss/3.x/cassandra/architecture/archDataDistributeReplication.html). For a production scenario this is an important part of the configuration since it may impact the overall performance of the queries. For sake of simplicity we will consider that only one datacenter is available which means that the replication class is set to `SimpleStrategy` and the replication factor is set to `1`.

One of the best practices of Apache Cassandra is to employ one table per query including just the necessary fields. The reason behind this is that Apache Cassandra works with rows partitioned by unique identifiers so-called [primary keys](https://www.datastax.com/blog/most-important-thing-know-cassandra-data-modeling-primary-key) that must be included in order inside the query. This, together with the absence of JOIN statements makes the usage of independent tables very benefitial as we will avoid query incompatibilities. [This](https://stackoverflow.com/questions/24949676/difference-between-partition-key-composite-key-and-clustering-key-in-cassandra) Stackoverflow post provides a good insight on this matter.

Making use of the `create_table` and `insert_query` methods included in `./lib.py` the tables `table1`, `table2` and `table3` corresponding to query1, query2 and query3 are generated with the relevant fields and primary keys and the data inserted. After each data insertion, each query is executed yielding the following results:

1. Query 1: `SELECT artist, song, length FROM table1 WHERE sessionId = 275 AND itemInSession = 4`

Output:

2. Query 2: `SELECT iteminsession, artist, song, firstName, lastName FROM table2 WHERE userid = 42 AND sessionid = 275`

Output:

3. Query 3: `SELECT firstName, lastName FROM table3 WHERE song = 'I Second That Emotion'`

Output:

### Requirements

* [pandas](https://pandas.pydata.org/getting_started.html)
* [NumPy](https://numpy.org/install/)
* [DataStax Driver for Apache Cassandra](https://github.com/datastax/python-driver#datastax-driver-for-apache-cassandra)
