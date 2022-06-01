package net.jgp.books.spark.ch07.lab600_xml_ingestion

import net.jgp.books.spark.basic.Basic

object XmlToDataframe extends Basic {
  def run(): Unit = {
    val spark = getSession("Text to Dataframe")

    val df = spark.read.format("xml").
      option("rowTag", "row").
      load("data/nasa-patents.xml")

    df.show(10)
    df.printSchema
  }
}