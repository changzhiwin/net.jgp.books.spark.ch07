package net.jgp.books.spark.ch07.lab300_csv_ingestion_with_schema;

// for StructType
import org.apache.spark.sql.types._

import net.jgp.books.spark.basic.Basic

object ComplexCsvToDataframe extends Basic {

  def run(mode: String): Unit = {

    println(s"------       Mode = $mode       ------")

    val schema = mode match {
      case "DDL" => formSchemaByDDL()
      case _     => formSchemaByStruct()
    }

    val spark = getSession("Complex CSV to Dataframe")

    val df = spark.read.format("csv").
      schema(schema).
      option("header", "true").
      option("multiline", "true").
      option("sep", ";").
      option("quote", "*").
      option("dateFormat", "M/d/y").
      // Inferring the schema is a costly operation
      // option("inferSchema", "true").
      load("./data/books.csv")

    df.show(5)
    df.printSchema

    spark.stop
  }

  private def formSchemaByDDL(): StructType = {
    StructType.fromDDL("id INT, authordId INT, bookTitle STRING, releaseDate DATE, url STRING")
  }

  private def formSchemaByStruct(): StructType = {
    StructType(
      Array(
        StructField("id", IntegerType, false),
        StructField("authordId", IntegerType, true),
        StructField("bookTitle", StringType,false),
        StructField("releaseDate", DateType, true),
        StructField("url", StringType, false)
      )
    )
  }
}