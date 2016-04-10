# will run as python script and ref all relevant actor tuples, just like the SQL query.
# to run: SPARK_CLASSPATH=/path/to/elasticsearch-hadoop-2.2.0/dist/elasticsearch-spark_2.11-2.2.0.jar ./spark-1.6.0-bin-hadoop2.6/bin/spark-submit 2.py

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from datetime import datetime

start = datetime.now()

# credit http://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks-in-python
def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

# set up context
sc = SparkContext("local[*]", "Simple App")
#sc = SparkContext("spark://url:7077", "Simple App")
sqlContext = SQLContext(sc)
sqlContext.setConf("spark.sql.shuffle.partitions", "5")

# issue movies query
conf = {"es.resource" : "movies2/logs", "es.query" : "?q=name:picture"}
movies = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

# place results in table
moviesRows = movies.map(lambda p: Row(id=int(p[1]['id']), name=p[1]['name']))
moviesRowsList = moviesRows.collect()
schemaMovies = sqlContext.createDataFrame(moviesRowsList)
schemaMovies.registerTempTable("movies")
sqlContext.cacheTable("movies")

# get ids in order to form acted_in query
ids = []
for moviesRow in moviesRowsList:
    ids.append(moviesRow['id'])
movieIdSnippets = []
for id in ids:
    movieIdSnippets.append("movie_id:" + str(id))

# partition acted_in query
actedInRowsTotalList = []
movieIdSnippetsChunks = list(chunks(movieIdSnippets, 1000))
for chunk in movieIdSnippetsChunks:
    movieIdQuery = " OR ".join(chunk)
    conf = {"es.resource" : "acted_in2/logs", "es.query" : "?q=" + movieIdQuery, "es.size" : "10000"}
    actedIn = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
        "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

    actedInRows = actedIn.map(lambda p: Row(actor_id=int(p[1]['actor_id']), movie_id=int(p[1]['movie_id']), role=p[1]['role']))
    actedInRowsTotalList.extend(actedInRows.collect())

# place results in acted_in table
schemaActedIn = sqlContext.createDataFrame(actedInRowsTotalList)
schemaActedIn.registerTempTable("acted_in")
sqlContext.cacheTable("acted_in")

# get ids for actor query
ids = sqlContext.sql("SELECT actor_id FROM acted_in")
actorSnippets = []
for id in ids.collect():
    actorSnippets.append("id:" + str(id.actor_id))

# partition actor query
actorRowsTotalList = []
actorSnippetsChunks = list(chunks(actorSnippets, 1000))
for chunk in actorSnippetsChunks:
    actorQuery = " OR ".join(chunk)
    conf = {"es.resource" : "actors2/logs", "es.query" : "?q=" + actorQuery, "es.size" : "10000"}
    actors = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
        "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

    actorsRows = actors.map(lambda p: Row(id=int(p[1]['id']), name=p[1]['name']))
    actorRowsTotalList.extend(actorsRows.collect())

# place in table
schemaActors = sqlContext.createDataFrame(actorRowsTotalList)
schemaActors.registerTempTable("actors")
sqlContext.cacheTable("actors")

# issue final query
s = "SELECT a.name AS Actors, m.name AS Movie, j.role AS Role FROM actors a LEFT JOIN acted_in j ON a.id = j.actor_id LEFT JOIN movies m ON j.movie_id = m.id"
results = sqlContext.sql(s)
resultsList = results.collect()
stop = datetime.now()
#for result in resultsList:
#    print(result)
diff = stop - start
print "TIME: ", diff.total_seconds() * 1000

