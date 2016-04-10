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

moviesDict = {}    
for movie in movies.collect():
    moviesDict[movie[1]["id"]] = movie[1]["name"]

# [Row(id=2905809), Row(id=2156022), Row(id=2491302), Row(id=2491697), Row(id=3480649), Row(id=3480651), Row(id=3480654), Row(id=3480662), Row(id=3480674), Row(id=545213), Row(id=392194), Row(id=392195), Row(id=928484), Row(id=1290613), Row(id=1735318), Row(id=1855959), Row(id=1999204), Row(id=2655049), Row(id=3091562), Row(id=3480644), Row(id=3480648), Row(id=3480653), Row(id=3480659), Row(id=3480660), Row(id=3480665), Row(id=3480666), Row(id=3480667), Row(id=3480671), Row(id=382313), Row(id=898641), Row(id=2383133), Row(id=1239674), Row(id=1797090), Row(id=2979333), Row(id=3134961), Row(id=3480645), Row(id=3480650), Row(id=3480652), Row(id=3480655), Row(id=3480657), Row(id=3480664), Row(id=3480669), Row(id=3480672), Row(id=3480675), Row(id=3480676), Row(id=3480677), Row(id=361432), Row(id=392193), Row(id=678008), Row(id=2153446), Row(id=1971374), Row(id=1301759), Row(id=3511488), Row(id=3480647), Row(id=3480656), Row(id=3480661), Row(id=3480663), Row(id=3480668), Row(id=3480670), Row(id=3480678), Row(id=352076), Row(id=763678), Row(id=715525), Row(id=898301), Row(id=2198544), Row(id=2423043), Row(id=2521999), Row(id=2629009), Row(id=2655048), Row(id=2655050), Row(id=1301466), Row(id=1333089), Row(id=1338241), Row(id=1999203), Row(id=1999205), Row(id=3054845), Row(id=3000459), Row(id=3000458), Row(id=3480646), Row(id=3480658), Row(id=3480673), Row(id=3537907), Row(id=3717129), Row(id=862226), Row(id=862227), Row(id=898640)]
movieIdSnippets = []
for id in moviesDict.keys():
    movieIdSnippets.append("movie_id:" + str(id))
movieIdQuery = " OR ".join(movieIdSnippets)

conf = {"es.resource" : "acted_in2/logs", "es.query" : "?q=" + movieIdQuery, "es.size" : "10000"}
actedIn = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

actedInDict = {}    
for acted in actedIn.collect():
    actedInDict[acted[1]["actor_id"]] = (acted[1]["movie_id"], acted[1]["role"])
    
# [Row(actor_id=889719), Row(actor_id=998217), Row(actor_id=1203538)]
actorSnippets = []
for id in actedInDict.keys():
    actorSnippets.append("id:" + str(id))
actorQuery = " OR ".join(actorSnippets)

conf = {"es.resource" : "actors2/logs", "es.query" : "?q=" + actorQuery, "es.size" : "10000"}
actors = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

actorsDict = {}    
for actor in actors.collect():
    actorsDict[actor[1]["id"]] = actor[1]["name"]
    
for key, value in actorsDict.iteritems():
    ai = actedInDict[key]
    movie = moviesDict[ai[0]]
    role = ai[1]
    print value + ", " + movie + ", " + role
