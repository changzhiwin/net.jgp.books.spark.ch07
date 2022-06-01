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

// special case for Avro format
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  --master "local[*]" \
  --jars jars/spark-avro_2.13-3.2.1.jar \
  target/scala-2.13/main-scala-ch7_2.13-1.0.jar AVRO
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
**Notice:** add --jar option.
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

### Case: Avro
**Notice:** add --jar option.
```
+------------+-------------+----+
|     station|         time|temp|
+------------+-------------+----+
|011990-99999|-619524000000|   0|
|011990-99999|-619506000000|  22|
|011990-99999|-619484400000| -11|
|012650-99999|-655531200000| 111|
|012650-99999|-655509600000|  78|
+------------+-------------+----+

root
 |-- station: string (nullable = true)
 |-- time: long (nullable = true)
 |-- temp: integer (nullable = true)
```

### Case: Orc
I use Spark 3.2.1. So no need change default setting `.config("spark.sql.orc.impl", "native")`
```
+-----+-----+-----+-------+-----+-----+-----+-----+-----+
|_col0|_col1|_col2|  _col3|_col4|_col5|_col6|_col7|_col8|
+-----+-----+-----+-------+-----+-----+-----+-----+-----+
|    1|    M|    M|Primary|  500| Good|    0|    0|    0|
|    2|    F|    M|Primary|  500| Good|    0|    0|    0|
|    3|    M|    S|Primary|  500| Good|    0|    0|    0|
|    4|    F|    S|Primary|  500| Good|    0|    0|    0|
|    5|    M|    D|Primary|  500| Good|    0|    0|    0|
|    6|    F|    D|Primary|  500| Good|    0|    0|    0|
|    7|    M|    W|Primary|  500| Good|    0|    0|    0|
|    8|    F|    W|Primary|  500| Good|    0|    0|    0|
|    9|    M|    U|Primary|  500| Good|    0|    0|    0|
|   10|    F|    U|Primary|  500| Good|    0|    0|    0|
+-----+-----+-----+-------+-----+-----+-----+-----+-----+
only showing top 10 rows

root
 |-- _col0: integer (nullable = true)
 |-- _col1: string (nullable = true)
 |-- _col2: string (nullable = true)
 |-- _col3: string (nullable = true)
 |-- _col4: integer (nullable = true)
 |-- _col5: string (nullable = true)
 |-- _col6: integer (nullable = true)
 |-- _col7: integer (nullable = true)
 |-- _col8: integer (nullable = true)
```

### Case: Parquet
```
+---+--------+-----------+------------+-------+----------+---------+----------+--------------------+----------+-------------------+
| id|bool_col|tinyint_col|smallint_col|int_col|bigint_col|float_col|double_col|     date_string_col|string_col|      timestamp_col|
+---+--------+-----------+------------+-------+----------+---------+----------+--------------------+----------+-------------------+
|  4|    true|          0|           0|      0|         0|      0.0|       0.0|[30 33 2F 30 31 2...|      [30]|2009-03-01 08:00:00|
|  5|   false|          1|           1|      1|        10|      1.1|      10.1|[30 33 2F 30 31 2...|      [31]|2009-03-01 08:01:00|
|  6|    true|          0|           0|      0|         0|      0.0|       0.0|[30 34 2F 30 31 2...|      [30]|2009-04-01 08:00:00|
|  7|   false|          1|           1|      1|        10|      1.1|      10.1|[30 34 2F 30 31 2...|      [31]|2009-04-01 08:01:00|
|  2|    true|          0|           0|      0|         0|      0.0|       0.0|[30 32 2F 30 31 2...|      [30]|2009-02-01 08:00:00|
|  3|   false|          1|           1|      1|        10|      1.1|      10.1|[30 32 2F 30 31 2...|      [31]|2009-02-01 08:01:00|
|  0|    true|          0|           0|      0|         0|      0.0|       0.0|[30 31 2F 30 31 2...|      [30]|2009-01-01 08:00:00|
|  1|   false|          1|           1|      1|        10|      1.1|      10.1|[30 31 2F 30 31 2...|      [31]|2009-01-01 08:01:00|
+---+--------+-----------+------------+-------+----------+---------+----------+--------------------+----------+-------------------+

root
 |-- id: integer (nullable = true)
 |-- bool_col: boolean (nullable = true)
 |-- tinyint_col: integer (nullable = true)
 |-- smallint_col: integer (nullable = true)
 |-- int_col: integer (nullable = true)
 |-- bigint_col: long (nullable = true)
 |-- float_col: float (nullable = true)
 |-- double_col: double (nullable = true)
 |-- date_string_col: binary (nullable = true)
 |-- string_col: binary (nullable = true)
 |-- timestamp_col: timestamp (nullable = true)
```

## 4, Some diffcult case

### Download jar
Search some key words. For example `spark-xml`.
> https://mvnrepository.com/

