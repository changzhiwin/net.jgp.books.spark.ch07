package net.jgp.books.spark.ch07.lab700_text_ingestion

import net.jgp.books.spark.basic.Basic

object TextToDataframe extends Basic {
  def run(): Unit = {
    val spark = getSession("Text to Dataframe")

    val df = spark.read.format("text").
      load("./data/romeo-juliet-pg1777.txt")

    df.show(10)
    df.printSchema
  }
}