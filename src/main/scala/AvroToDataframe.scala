package net.jgp.books.spark.ch07.lab900_avro_ingestion

import net.jgp.books.spark.basic.Basic

object AvroToDataframe extends Basic {
  def run(): Unit = {
    val spark = getSession("Avro to Dataframe")

    val df = spark.read.format("avro").
      load("./data/weather.avro")

    df.show(10)
    df.printSchema

    println(s"The dataframe has ${df.count()} rows.");
  }
}