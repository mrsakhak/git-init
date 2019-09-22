

import org.json4s.{Formats, FullTypeHints, jackson}
import org.apache.spark.sql.SparkSession

object JsonReader extends App {
  case class Wine(
     id: Option[Int],
     country: Option[String],
     points: Option[Int],
     price: Option[Float],
     title: Option[String],
     variety: Option[String],
     winery: Option[String])
  implicit val formats = jackson.Serialization.formats(FullTypeHints(List(classOf[Wine])))

  val spark = SparkSession
    .builder()
    .master(master= "local[*]")
    .getOrCreate()
  val sc = spark.sparkContext
  val jsonFolder = args(0)
  val jsonData = sc.textFile(jsonFolder).toJavaRDD().rdd

  jsonData
    .map( x => jackson.JsonMethods.parse(x).extract[Wine] )
    .foreach( wine => println(wine.toString) )


  println(formats)

}