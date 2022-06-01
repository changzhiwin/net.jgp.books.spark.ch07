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
  target/scala-2.13/main-scala-ch7_2.13-1.0.jar
```

## 3, print

### Case: csv
```
------       Mode = DDL       ------

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

## 4, Some diffcult case

### Two ways to define a schema
Detail from <Learning Spark Lightning> chapter 3 about `The DataFrame API`.
```
// 1, DDL
    StructType.fromDDL("id INT, authordId INT, bookTitle STRING, releaseDate DATE, url STRING")

// 2, StructType
    StructType(
      Array(
        StructField("id", IntegerType, false),
        StructField("authordId", IntegerType, true),
        StructField("bookTitle", StringType,false),
        StructField("releaseDate", DateType, true),
        StructField("url", StringType, false)
      )
    )
```

