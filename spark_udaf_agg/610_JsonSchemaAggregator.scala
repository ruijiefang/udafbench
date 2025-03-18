//https://raw.githubusercontent.com/vincentdoba/blog-examples/f67f881d3c932bc1fad0e914eb093d74db6d17fd/20211005-json-schema-aggregator/src/main/scala/fr/doba/vincent/aggregator/JsonSchemaAggregator.scala
package fr.doba.vincent.aggregator

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

object JsonSchemaAggregator extends Aggregator[String, Map[String, String], String] {

  override def zero: Map[String, String] = Map.empty[String, String]

  override def reduce(currentSchema: Map[String, String], json: String): Map[String, String] = {
    merge(SparkSchemaParser.fromJson(json), currentSchema)
  }

  override def merge(schema1: Map[String, String], schema2: Map[String, String]): Map[String, String] = {
    (schema1.toSeq ++ schema2.toSeq).groupBy(_._1).map(elem => {
      if (elem._1.endsWith("[]") && elem._2.head._2 == "STRING") {
        elem._2.last
      } else {
        elem._2.head
      }
    })
  }

  override def finish(reduction: Map[String, String]): String = toDDL(reduction.toList)

  override def bufferEncoder: Encoder[Map[String, String]] = ExpressionEncoder[Map[String, String]]

  override def outputEncoder: Encoder[String] = Encoders.STRING

  def toDDL(schema: List[(String, String)]): String = if (schema.forall(_._1.startsWith("[]"))) {
    schema.groupBy(_._1.split("\\.").head).map {
      case ("[]", ("[]", datatype) :: Nil) => datatype
      case (_, list) => toDDL(dropPrefix(list).filter(_ != ("", "STRING")))
    }.mkString("ARRAY<", ", ", ">")
  } else {
    schema.groupBy(_._1.split("\\.").head).map {
      case (name, (_, datatype) :: Nil) if name.endsWith("[]") => s"`${name.dropRight(2)}`: ARRAY<$datatype>"
      case (name, (_, datatype) :: Nil) => s"`$name`: $datatype"
      case (name, list) if name.endsWith("[]") => s"`${name.dropRight(2)}`: ARRAY<${toDDL(dropPrefix(list).filter(_ != ("", "STRING")))}>"
      case (name, list) => s"`$name`: ${toDDL(dropPrefix(list))}"
    }.mkString("STRUCT<", ", ", ">")
  }

  private def dropPrefix(list: List[(String, String)]): List[(String, String)] = {
    list.map(elem => (elem._1.split("\\.").tail.mkString("."), elem._2))
  }

}
