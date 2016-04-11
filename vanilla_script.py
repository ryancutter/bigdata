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

sc = SparkContext("local[*]", "Simple App")
sc = SparkContext("spark://url:7077", "Simple App")
sqlContext = SQLContext(sc)

conf = {"es.resource" : "movies2/logs", "es.query" : "?q=name:blue"}
movies = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

moviesDict = {}
for movie in movies.collect():
    moviesDict[movie[1]["id"]] = movie[1]["name"]

# get ids in order to form acted_in query
movieIdSnippets = []
for id in moviesDict.keys():
    movieIdSnippets.append("movie_id:" + str(id))

actedInDict = {}   
for chunk in list(chunks(movieIdSnippets, 1000)):
    movieIdQuery = " OR ".join(chunk)
    conf = {"es.resource" : "acted_in2/logs", "es.query" : "?q=" + movieIdQuery, "es.size" : "10000"}
    actedIn = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
        "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)
        
    for acted in actedIn.collect():
        actedInDict[acted[1]["actor_id"]] = (acted[1]["movie_id"], acted[1]["role"])

actorSnippets = []
for id in actedInDict.keys():
    actorSnippets.append("id:" + str(id))

actorsDict = {}
for chunk in list(chunks(actorSnippets, 1000)):
    actorQuery = " OR ".join(chunk)
    conf = {"es.resource" : "actors2/logs", "es.query" : "?q=" + actorQuery, "es.size" : "10000"}
    actors = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
        "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

    for actor in actors.collect():
        actorsDict[actor[1]["id"]] = actor[1]["name"]
        
result = []
for key, value in actorsDict.iteritems():
    ai = actedInDict[key]
    movie = moviesDict[ai[0]]
    role = ai[1]
    result.append(value + ", " + movie + ", " + role)
stop = datetime.now()
#for res in result:
#    print result
diff = stop - start
print "TIME: ", diff.total_seconds() * 1000
print len(result)
