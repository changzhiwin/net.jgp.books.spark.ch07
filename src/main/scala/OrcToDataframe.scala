package net.jgp.books.spark.ch07.lab910_orc_ingestion

import net.jgp.books.spark.basic.Basic

object OrcToDataframe extends Basic {
  def run(): Unit = {
    val spark = getSession("ORC to Dataframe")

    val df = spark.read.format("orc").
      load("./data/demo-11-zlib.orc")

    df.show(10)
    df.printSchema

    println(s"The dataframe has ${df.count()} rows.");
  }
}