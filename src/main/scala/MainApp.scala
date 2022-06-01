package net.jgp.books.spark

import net.jgp.books.spark.ch07.lab300_csv_ingestion_with_schema.ComplexCsvToDataframe
import net.jgp.books.spark.ch07.lab600_xml_ingestion.XmlToDataframe
import net.jgp.books.spark.ch07.lab700_text_ingestion.TextToDataframe
import net.jgp.books.spark.ch07.lab500_json_ingestion.JsonToDataframe

import net.jgp.books.spark.ch07.lab900_avro_ingestion.AvroToDataframe
import net.jgp.books.spark.ch07.lab910_orc_ingestion.OrcToDataframe
import net.jgp.books.spark.ch07.lab930_parquet_ingestion.ParquetToDataframe

object MainApp {
  def main(args: Array[String]) = {

    val (whichCase, otherArg) = args.length match {
      case 1 => (args(0).toUpperCase, "")
      case 2 => (args(0).toUpperCase, args(1).toUpperCase)
      case _ => ("TEXT", "")
    }

    println(s"=========== whichCase = $whichCase, otherArg = $otherArg ===========")

    whichCase match {
      case "AVRO" => AvroToDataframe.run()
      case "ORC"  => OrcToDataframe.run()
      case "PAR"  => ParquetToDataframe.run()
      case "JSON" => JsonToDataframe.run(otherArg)        // M or other
      case "XML"  => XmlToDataframe.run()
      case "CSV"  => ComplexCsvToDataframe.run(otherArg)  // DDL or other
      case _      => TextToDataframe.run()
    }
  }
}