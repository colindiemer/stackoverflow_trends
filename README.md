# Trend Tracker: Stackoverflow and the changing trends in technology. 

Stackoverflow is, in a way, more than just a Q&A site for coders: it steadily documents the way coders use language.
The goal of this project was to extract keywords from Stackoverflow posts and determine which keywords are associated with which tags, and track how often these keywords are used over time. 

The goal is to provide a user with a dashboard-like interface to investigate questions like: "are rdd's being used less in Spark" or more generally "some technology seems to be rising or falling in popularity, what particular features are its users posting about?".

![Sample Usage: The rise of Panda(s) among Python posters](https://i.imgur.com/qk3VeTM.png)

*Sample Usage: The rise of Panda(s) among Python posters*

The data used was the entire 70GB file of stackoverflow posts at the data dump: https://archive.org/details/stackexchange. The code base assumes you have moved the files for Posts and Tags to your own S3 bucket in files `stackoverflow/Posts.xml` and `stackoverflow/Tags.xml`. 

Stackexchange enforces a consistent schema amongst the data dumps of it's various sites. In particular, the code will work as expected on any site within the stackexchange network. However, only Stackoverflow has enough user activity to have an archive in the tens of GBs, and so the Spark-job run here is likely overkill on smaller sites. 

## Tech Stack / Pipeline

The files of posts and tags are stored in S3. The data transformation process takes place with PySpark. Benchmarking was done using both Redis and Cassandra as a database for the output tables. Accordingly, code for both is included, although Cassandra is preferred. Dash/Plotly apps then queries these databases to produce output graphs. 


![Pipeline](https://i.imgur.com/dlZ2823.png)

## Environment Variables

The following environment variables are used by the transformation `application.py` and the respective Redis/Cassandra front-end Dash/Plotly applications call their respective environment variables too. 

* `S3_BUCKET` - an s3a link to your S3 bucket containing `stackoverflow/Posts.xml` and `stackoverflow/Tags.xml`.  
* `REDIS_DNS` - an address for the Redis Server
* `REDIS_DB` - a number of the database on the Redis Server (typically 0).
* `CASSANDRA_DNS` - an address for the Cassandra Server/Cluster. 
* `CASSANDRA_PORT` - a port for the Cassandra Server/Cluster (typically 9042).
* `CASSANDRA_KEYSPACE` - a name for keyspace used on Cassandra. 
* `CASSANDRA_TABLE` - a name for the particular Cassandra table. 

## The Data Transformation Process

First, the data set is filtered by those posts given a specified tag (e.g. posts marked "Python"). Answers are associated with the tags of their associated question. The title (if present) and body are combined into the "text" of the post. Then the following text cleaning is performed:

* Punctuation is removed (Sorry `C++`, you now become the same as `C`!)
* Words are made lower-case
* Text contained in markup tags (such as code) is removed
* Lemmatization (So `pandas` is now `panda`)
* Stopwords are removed (this is a delicate issue for this project, more on this later!)
* Text is tokenized

From there, TF-IDF scores were computed. In practice, I found the computation of IDF scores using SparkML's built in IDF disappointingly slow, especially since the process must be run for any tag one wants to consider. Instead, I used an IDF lookup table from the python package `wikiwords`, effectively using IDF scores from common language usage. Because tech words are in much more prevalent use on StackOverflow than on Wikipedia, using these IDF lookups requires some tweaking of stopwords. 

## Stopwords

...are a pain. StackOverflow has some idiosyncratic usage of words and language. 

`transformation.py` will look for two `.txt` files in the same directory: `stopwords_file` (default `stopwords_10000.txt` and the other for `common_tech_words_file` (default `common_tech_words.txt`). The former of these is recommended to be a fairly substantial list of commonly used words. 

The problem is that any sufficiently long list of common words will likely contain some interesting tech words (e.g. 'list', 'class') one may not want to remove. So `transformation.py` looks into the list of the most popular tags, and removes words appearing there from our large list of stopwords. Regretfully, there are some ridiculously broad tags in use on StackOverflow (e.g. "arrays). The `common_tech_words_file` then re-includes these in our list of stopwords. In this way, we get a fairly balanced mixed of interesting, but not overly common, tech words.

Code for the slower pipeline which includes the actual direct IDF calculation is including for benchmarking purposes. It is an interesting data analytics question as to how closely we can match these two keyword extraction techniques via delicate adjustment of stopwords with a general usage IDF lookup. 

## Usage and Dependencies

The main spark job file is `tranformation.py` in the `spark` directory. This file has dependencies on `XML_parser.py`, `generate_stopwords.py` and `process_tags.py`, so make sure that these files are included with the `--py-files` flag when running your `spark-submit`.

To write to a Redis database, your `spark-submit` will require a JAR from https://mvnrepository.com/artifact/com.redislabs/spark-redis/2.3.0 . On your Spark Cluster, you should thus add the following to your `spark-submit`: `--jars /home/ubuntu/spark-redis/target/spark-redis-2.4.1-SNAPSHOT-jar-with-dependencies.jar` depending on where you downloaded the JAR to your local file. 

To write to a Cassandra database, your `spark-submit` will require a JAR from datastax. This can be downloaded directly with the spark job if one adds the following to your `spark-submit`: `--packages datastax:spark-cassandra-connector:2.4.0-s_2.11`. 







 
