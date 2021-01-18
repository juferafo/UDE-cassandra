# UDE-cassandra

The goal of this repository is to analyze activity data that is similar to the information present in the logs generated in well known streming apps. Such information includes speciphic user data like, for example, user location, number of songs listened or how often the user listens to music. These details are crucial for companies that offer streaming services since, based on its analysis, it is possible to get insights that relate to user behavior and make business decissions. 

In order to achieve this goal it is necessary to implement a "playground" that makes possible the analysis of the data. The design proposed here employes the open-source software [Apache Cassandra](https://cassandra.apache.org/) to create a [NoSQL](https://www.xenonstack.com/blog/nosql-databases/) database where to store and manage data. As one can imagine, there are multiple ways to achieve this goal. Depending on the size of the data we could consider the benefits of using a SQL database. The work presented here can be thought as a continuation/evolution of the use-case presented in [this repository](https://github.com/juferafo/UDE-postgres).

### Activity data

The raw data can be found in the directory "./event_data" that contains CSV files with user activity:

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

In order to process all this information we will build an ETL (extract, transform, load) pipeline that brings the CSV data all-together into a database. Due to the nature of this use-case it is expected that in a real scenario for widely used applications terabytes of data are generated. In these situations it is more convenient to employ a NoSQL model instead the regular SQL ones. We will use [Apache Cassandra](https://github.com/apache/cassandra) to host the database since it is a highly-scalable partitioned row store. This means that the data is organized in rows and columns (like in the SQL model) and partitioned by an unique key/identifier.  

In Apache Cassandra the queries are executed in the Cassandra Query Language [CQL](https://cassandra.apache.org/doc/latest/cql/) which is very similar to SQL. However, JOIN and GOUP BY statements do not exist in Apache Cassandra and, therefore, the data must undergo a process of [denormalization](https://www.datastax.com/blog/basic-rules-cassandra-data-modeling). It is very important to have in mind that the database will be modeled according to the queries and not the other way around.

### Code workflow

#### Step 1: preprocessing the data

#### Step 2: database configuration

### Requirements

* [pandas](https://pandas.pydata.org/getting_started.html)
* [NumPy](https://numpy.org/install/)
* [DataStax Driver for Apache Cassandra](https://github.com/datastax/python-driver#datastax-driver-for-apache-cassandra)
