# pyspark terminal commands.  will not run as python script.  will only ref a single actor tuple.

from pyspark.sql import SQLContext, Row

conf = {"es.resource" : "movies2/logs", "es.query" : "?q=name:bourne"}
movies = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

moviesRows = movies.map(lambda p: Row(id=int(p[1]['id']), name=p[1]['name']))
schemaMovies = sqlContext.createDataFrame(moviesRows)
schemaMovies.registerTempTable("movies")
ids = sqlContext.sql("SELECT id FROM movies")
# [Row(id=2905809), Row(id=2156022), Row(id=2491302), Row(id=2491697), Row(id=3480649), Row(id=3480651), Row(id=3480654), Row(id=3480662), Row(id=3480674), Row(id=545213), Row(id=392194), Row(id=392195), Row(id=928484), Row(id=1290613), Row(id=1735318), Row(id=1855959), Row(id=1999204), Row(id=2655049), Row(id=3091562), Row(id=3480644), Row(id=3480648), Row(id=3480653), Row(id=3480659), Row(id=3480660), Row(id=3480665), Row(id=3480666), Row(id=3480667), Row(id=3480671), Row(id=382313), Row(id=898641), Row(id=2383133), Row(id=1239674), Row(id=1797090), Row(id=2979333), Row(id=3134961), Row(id=3480645), Row(id=3480650), Row(id=3480652), Row(id=3480655), Row(id=3480657), Row(id=3480664), Row(id=3480669), Row(id=3480672), Row(id=3480675), Row(id=3480676), Row(id=3480677), Row(id=361432), Row(id=392193), Row(id=678008), Row(id=2153446), Row(id=1971374), Row(id=1301759), Row(id=3511488), Row(id=3480647), Row(id=3480656), Row(id=3480661), Row(id=3480663), Row(id=3480668), Row(id=3480670), Row(id=3480678), Row(id=352076), Row(id=763678), Row(id=715525), Row(id=898301), Row(id=2198544), Row(id=2423043), Row(id=2521999), Row(id=2629009), Row(id=2655048), Row(id=2655050), Row(id=1301466), Row(id=1333089), Row(id=1338241), Row(id=1999203), Row(id=1999205), Row(id=3054845), Row(id=3000459), Row(id=3000458), Row(id=3480646), Row(id=3480658), Row(id=3480673), Row(id=3537907), Row(id=3717129), Row(id=862226), Row(id=862227), Row(id=898640)]
movieIdSnippets = []
for id in ids.collect():
    movieIdSnippets.append("movie_id:" + str(id.id))
movieIdQuery = " OR ".join(movieIdSnippets)

conf = {"es.resource" : "acted_in2/logs", "es.query" : "?q=" + movieIdQuery, "es.size" : "10000"}
actedIn = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)
    
actedInRows = actedIn.map(lambda p: Row(actor_id=int(p[1]['actor_id']), movie_id=int(p[1]['movie_id']), role=p[1]['role']))
schemaActedIn = sqlContext.createDataFrame(actedInRows)
schemaActedIn.registerTempTable("acted_in")
ids = sqlContext.sql("SELECT actor_id FROM acted_in")
# [Row(actor_id=889719), Row(actor_id=998217), Row(actor_id=1203538)]
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
# Row(Actors=u'Lenhardt Troy', Movie=u'The Bourne Identity 2002', Role=u'Marine')


# equivalent SQL query
SELECT a.name AS Actor, m.name AS Movie, j.role AS Role FROM actors a LEFT JOIN acted_in j ON a.id = j.actor_id INNER JOIN movies m ON j.movie_id = m.id AND to_tsvector('english', m.name) @@ to_tsquery('english', 'bourne');


# timing
# movies
16/03/20 10:38:43 INFO DAGScheduler: Job 27 finished: take at SerDeUtil.scala:201, took 0.021953 s
16/03/20 10:39:27 INFO DAGScheduler: Job 28 finished: runJob at PythonRDD.scala:393, took 0.025412 s
16/03/20 10:39:54 INFO DAGScheduler: Job 29 finished: collect at <stdin>:1, took 0.075773 s

# acted_in
16/03/20 10:40:15 INFO DAGScheduler: Job 31 finished: take at SerDeUtil.scala:201, took 0.020859 s
16/03/20 10:40:34 INFO DAGScheduler: Job 33 finished: runJob at PythonRDD.scala:393, took 0.041061 s
16/03/20 10:40:54 INFO DAGScheduler: Job 34 finished: collect at <stdin>:1, took 0.037302 s

# actors & join
16/03/20 10:41:12 INFO DAGScheduler: Job 36 finished: take at SerDeUtil.scala:201, took 0.023502 s
16/03/20 10:41:28 INFO DAGScheduler: Job 38 finished: runJob at PythonRDD.scala:393, took 0.030813 s
16/03/20 10:42:00 INFO DAGScheduler: Job 39 finished: collect at <stdin>:1, took 0.886844 s

ES: 0.064s
Spark: 1.093s
Total: 1.157s
