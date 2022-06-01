package net.jgp.books.spark.ch07.lab930_parquet_ingestion

import net.jgp.books.spark.basic.Basic

object ParquetToDataframe extends Basic {
  def run(): Unit = {
    val spark = getSession("Parquet to Dataframe")

    val df = spark.read.format("parquet").
      load("./data/alltypes_plain.parquet")

    df.show(10)
    df.printSchema

    println(s"The dataframe has ${df.count()} rows.");
  }
}