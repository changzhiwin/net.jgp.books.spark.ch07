package net.jgp.books.spark

import net.jgp.books.spark.ch07.lab300_csv_ingestion_with_schema.ComplexCsvToDataframe

object MainApp {
  def main(args: Array[String]) = {

    // struct by default
    val fristArg = (args.toSeq :+ "struct")(0).toUpperCase()
    
    // Case: ComplexCsvToDataframe
    ComplexCsvToDataframe.run(fristArg)

  }
}