# to run: SPARK_CLASSPATH=/path/to/elasticsearch-hadoop-2.2.0/dist/elasticsearch-spark_2.11-2.2.0.jar ./spark-1.6.0-bin-hadoop2.6/bin/spark-submit 2.py

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

sc = SparkContext("local[*]", "Simple App")
#sc = SparkContext("spark://url:7077", "Simple App")
sqlContext = SQLContext(sc)

conf = {"es.resource" : "movies2/logs", "es.query" : "?q=name:bourne"}
movies = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

moviesRows = movies.map(lambda p: Row(id=int(p[1]['id']), name=p[1]['name']))
schemaMovies = sqlContext.createDataFrame(moviesRows)
schemaMovies.registerTempTable("movies")
ids = sqlContext.sql("SELECT id FROM movies")
ids.collect()

conf = {"es.resource" : "acted_in2/logs", "es.query" : "?q=movie_id:3480646"}
actedIn = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

actedInRows = actedIn.map(lambda p: Row(actor_id=int(p[1]['actor_id']), movie_id=int(p[1]['movie_id']), role=p[1]['role']))
schemaActedIn = sqlContext.createDataFrame(actedInRows)
schemaActedIn.registerTempTable("acted_in")
ids = sqlContext.sql("SELECT actor_id FROM acted_in")
ids.collect()

conf = {"es.resource" : "actors2/logs", "es.query" : "?q=id:1203538"}
actors = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

actorsRows = actors.map(lambda p: Row(id=int(p[1]['id']), name=p[1]['name']))
schemaActors = sqlContext.createDataFrame(actorsRows)
schemaActors.registerTempTable("actors")
s = "SELECT a.name AS Actors, m.name AS Movie, j.role AS Role FROM actors a LEFT JOIN acted_in j ON a.id = j.actor_id LEFT JOIN movies m ON j.movie_id = m.id"
results = sqlContext.sql(s)
for result in results.collect():
    print(result)
