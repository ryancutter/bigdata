# will run as python script and ref all relevant actor tuples, just like the SQL query.
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
moviesRowsList = moviesRows.collect()
ids = []
for moviesRow in moviesRowsList:
    ids.append(moviesRow['id'])
schemaMovies = sqlContext.createDataFrame(moviesRowsList)
schemaMovies.registerTempTable("movies")
movieIdSnippets = []
for id in ids:
    movieIdSnippets.append("movie_id:" + str(id))
movieIdQuery = " OR ".join(movieIdSnippets)

conf = {"es.resource" : "acted_in2/logs", "es.query" : "?q=" + movieIdQuery, "es.size" : "10000"}
actedIn = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

actedInRows = actedIn.map(lambda p: Row(actor_id=int(p[1]['actor_id']), movie_id=int(p[1]['movie_id']), role=p[1]['role']))
schemaActedIn = sqlContext.createDataFrame(actedInRows)
schemaActedIn.registerTempTable("acted_in")
ids = sqlContext.sql("SELECT actor_id FROM acted_in")
actorSnippets = []
for id in ids.collect():
    actorSnippets.append("id:" + str(id.actor_id))
actorQuery = " OR ".join(actorSnippets)

conf = {"es.resource" : "actors2/logs", "es.query" : "?q=" + actorQuery, "es.size" : "10000"}
actors = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

actorsRows = actors.map(lambda p: Row(id=int(p[1]['id']), name=p[1]['name']))
schemaActors = sqlContext.createDataFrame(actorsRows)
schemaActors.registerTempTable("actors")
s = "SELECT a.name AS Actors, m.name AS Movie, j.role AS Role FROM actors a LEFT JOIN acted_in j ON a.id = j.actor_id LEFT JOIN movies m ON j.movie_id = m.id"
results = sqlContext.sql(s)
for result in results.collect():
    print(result)