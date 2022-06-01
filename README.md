# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch07

# Environment
- Java 11
- Scala 2.13.8
- Spark 3.2.1

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch7_2.13-1.0.jar` (the same as name property in sbt file)

## 2, submit jar file, in project root dir
```
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  --master "local[*]" \
  target/scala-2.13/main-scala-ch7_2.13-1.0.jar JSON M

// special case for xml
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  --master "local[*]" \
  --jars jars/spark-xml_2.13-0.14.0.jar \
  target/scala-2.13/main-scala-ch7_2.13-1.0.jar XML
```

## 3, print

### Case: csv
```
+---+---------+--------------------+-----------+--------------------+
| id|authordId|           bookTitle|releaseDate|                 url|
+---+---------+--------------------+-----------+--------------------+
|  1|        1|Fantastic Beasts ...| 2016-11-18|http://amzn.to/2k...|
|  2|        1|Harry Potter and ...| 2015-10-06|http://amzn.to/2l...|
|  3|        1|The Tales of Beed...| 2008-12-04|http://amzn.to/2k...|
|  4|        1|Harry Potter and ...| 2016-10-04|http://amzn.to/2k...|
|  5|        2|Informix 12.10 on...| 2017-04-23|http://amzn.to/2i...|
+---+---------+--------------------+-----------+--------------------+
only showing top 5 rows

root
 |-- id: integer (nullable = true)
 |-- authordId: integer (nullable = true)
 |-- bookTitle: string (nullable = true)
 |-- releaseDate: date (nullable = true)
 |-- url: string (nullable = true)
```

### Case: json line
```
root
 |-- datasetid: string (nullable = true)
 |-- fields: struct (nullable = true)
 |    |-- address: string (nullable = true)
 |    |-- geocode: array (nullable = true)
 |    |    |-- element: double (containsNull = true)
 |    |-- parcel_number: string (nullable = true)
 |    |-- year: string (nullable = true)
 |-- geometry: struct (nullable = true)
 |    |-- coordinates: array (nullable = true)
 |    |    |-- element: double (containsNull = true)
 |    |-- type: string (nullable = true)
 |-- record_timestamp: string (nullable = true)
 |-- recordid: string (nullable = true)
```

### Case: json multiline
```
root
 |-- destination_description: string (nullable = true)
 |-- entry_exit_requirements: string (nullable = true)
 |-- geopoliticalarea: string (nullable = true)
 |-- health: string (nullable = true)
 |-- iso_code: string (nullable = true)
 |-- last_update_date: string (nullable = true)
 |-- local_laws_and_special_circumstances: string (nullable = true)
 |-- safety_and_security: string (nullable = true)
 |-- tag: string (nullable = true)
 |-- travel_embassyAndConsulate: string (nullable = true)
 |-- travel_transportation: string (nullable = true)
```

### Case: xml
```
root
 |-- __address: string (nullable = true)
 |-- __id: long (nullable = true)
 |-- __position: long (nullable = true)
 |-- __uuid: string (nullable = true)
 |-- application_sn: string (nullable = true)
 |-- case_number: string (nullable = true)
 |-- center: string (nullable = true)
 |-- patent_expiration_date: timestamp (nullable = true)
 |-- patent_number: string (nullable = true)
 |-- status: string (nullable = true)
 |-- title: string (nullable = true)
```

### Case: text
```
+--------------------+
|               value|
+--------------------+
|                    |
|This Etext file i...|
|cooperation with ...|
|Future and Shakes...|
|Etexts that are N...|
|                    |
|*This Etext has c...|
|                    |
|<<THIS ELECTRONIC...|
|SHAKESPEARE IS CO...|
+--------------------+
only showing top 10 rows

root
 |-- value: string (nullable = true)
```

## 4, Some diffcult case

### Download jar
Search some key words. For example `spark-xml`.
> https://mvnrepository.com/

