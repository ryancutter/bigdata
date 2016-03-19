# given 3 ES collections (company, person, worked_at), let's say the user wants to know all people who worked
# at a company with 'Acme' in the name

# this workflow take from http://spark.apache.org/docs/latest/sql-programming-guide.html#interoperating-with-rdds

from pyspark.sql import SQLContext, Row

# get company
conf = {"es.resource" : "cu/company", "es.query" : "?q=name:acme"}

# run the query through ES-Hadoop
company = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

# map data to rows
companyRows = company.map(lambda p: Row(id=int(p[0]), name=p[1]['name']))

# register dataframe as table
schemaCompany = sqlContext.createDataFrame(companyRows)
schemaCompany.registerTempTable("company")

# get the company ids we're interested in
# possibly don't need to get this via SQL command, don't know
ids = sqlContext.sql("SELECT id FROM company")
ids.collect()

# get worked_at
# TODO fix query
conf = {"es.resource" : "cu/worked_at", "es.query" : "?q=companyID:2001"}

# run the query through ES-Hadoop
workedAt = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

# map data to rows
workedAtRows = workedAt.map(lambda p: Row(id=int(p[0]), personID=int(p[1]['personID']), companyID=int(p[1]['companyID']), period=p[1]['period']))

# register dataframe as table
schemaWorkedAt = sqlContext.createDataFrame(workedAtRows)
schemaWorkedAt.registerTempTable("worked_at")

# get the person ids we're interested in
ids = sqlContext.sql("SELECT personID FROM worked_at")
ids.collect()

# get person
# TODO fix query
conf = {"es.resource" : "cu/person", "es.query" : "?q=*:*"}

# run the query through ES-Hadoop
person = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",\
    "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)

# map data to rows
personRows = person.map(lambda p: Row(id=int(p[0]), name=p[1]['name']))

# register dataframe as table
schemaPerson = sqlContext.createDataFrame(personRows)
schemaPerson.registerTempTable("person")

# all the data is in the table, run the SQL JOINs to get final result
s = "SELECT p.name AS Person, c.name AS Company, w.period FROM person p LEFT JOIN worked_at w ON p.id = w.personID LEFT JOIN company c ON w.companyID = c.id"
results = sqlContext.sql(s)

# print results
for result in results.collect():
  print(result)

# yields:
#Row(Person=u'John Smith', Company=u'Acme, Inc.', period=u'2008-2013')
#Row(Person=u'Mike Jones', Company=u'Acme, Inc.', period=u'2000-2016')
