# Trend Tracker: Stackoverflow and the changing trends in technology. 

Stackoverflow is, in a way, more than just a Q&A site for coders: it steadily documents the way coders use language.
The goal of this project was to extract keywords from Stackoverflow posts and determine which keywords are associated with which tags, and track how often these keywords are used over time. 

The goal is to provide a user with a dashboard-like interface to investigate questions like: "are rdd's being used less in Spark" or "some technology seems to be rising, what particular topics are its users posting about?".

The data used was the entire 70GB file of stackoverflow posts at the data dump: https://archive.org/details/stackexchange. The code base assumes you have moved the files for Posts and Tags to your own S3 bucket in files `stackoverflow/Posts.xml` and `stackoverflow/Tags.xml`. 

Stackexchange enforces a consistent schema amongst the data dumps of it's various sites. In particular, the code will work as expected on any site within the stackexchange network. However, only Stackoverflow has enough user activity to have an archive in the tens of GBs, and so the Spark-job run here is likely overkill on smaller sites. 

## Tech Stack

The files of posts and tags are stores in S3. The data transformation process takes place with PySpark. Benchmarking was done using both Redis and Cassandra as a database for the output tables. Accordingly, code for both is included. Dash/Plotly apps then queries these databases to produce output graphs. 

Of course, in practice there is no reason to use both Redis and Cassandra simultaneously; the user is invited to pick a favorite and only use that. 

## Environment Variables and Dependencies. 

The following environment variables are used. 

* `S3_BUCKET` - an s3a link to your S3 bucket containing `stackoverflow/Posts.xml` and `stackoverflow/Tags.xml`.  
* `REDIS_DNS` - an address for the Redis Server
* `REDIS_DB` - a number of the database on the Redis Server (typically 0).
* `CASSANDRA_DNS` - an address for the Cassandra Server/Cluster. 
* `CASSANDRA_PORT` - a port for the Cassandra Server/Cluster (typically 9042).
* `CASSANDRA_KEYSPACE` - a name for keyspace used on Cassandra. 
* `CASSANDRA_TABLE` - a name for the particular Cassandra table. 






 
