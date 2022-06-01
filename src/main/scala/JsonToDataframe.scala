package net.jgp.books.spark.ch07.lab500_json_ingestion

import net.jgp.books.spark.basic.Basic

object JsonToDataframe extends Basic {
  def run(multi: String): Unit = {
    val spark = getSession("Json to Dataframe")

    val (fileName, isMulti) = multi match {
      case "M" => ("data/countrytravelinfo.json", true)
      case _   => ("data/durham-nc-foreclosure-2006-2016.json", false)
    }

    val df = spark.read.format("json").
      option("multiline", isMulti).
      load(fileName)

    df.show(10)
    df.printSchema
  }
}