package obalitskyi.gridu.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object App {

  def main(args: Array[String]) {
    val eventsPath = args(0)
    val ipPath = args(1)
    val ipinfoPath = args(2)

    val spark = SparkSession
      .builder()
      .appName("griduCapstone")
      //.config("spark.executor.memory","480m")
      //.config("spark.executor.cores","1")
      //.master("local[12]")
      .getOrCreate()

    val eventsSchema = StructType(Array(
      StructField("product", StringType),
      StructField("price", DoubleType),
      StructField("eventDate", StringType),
      StructField("category", StringType),
      StructField("ip", StringType)
    ))

    val events = spark.read.schema(eventsSchema).csv(eventsPath)
    val ip = spark.read.option("header", "true").csv(ipPath)
    val ipinfo = spark.read.option("header", "true").csv(ipinfoPath)

    events.createTempView("events")
    ip.createTempView("ip")
    ipinfo.createTempView("ipinfo")

    val top10ByCategory = new Top10ByCategory()
    val top10ProductsInEachCategory = new Top10ProductsInEachCategory()
    val top10CountriesByMoneySpending = new Top10CountriesByMoneySpending()

    events.cache()

    args(3) match {
      case u if u.equals("1") || u.toLowerCase().contains("top10bycategory") =>
        top10ByCategory.run(events, spark)
      case u if u.equals("2") || u.toLowerCase().contains("top10productsineachcategory") =>
        top10ProductsInEachCategory.run(events, spark)
      case u if u.equals("3") || u.toLowerCase().contains("top10countriesbymoneyspending") =>

        ip.cache()
        ipinfo.cache()

        top10CountriesByMoneySpending.run(events, ip, ipinfo, spark)
      case _ =>

        ip.cache()
        ipinfo.cache()

        top10ByCategory.run(events, spark)
        top10ProductsInEachCategory.run(events, spark)
        top10CountriesByMoneySpending.run(events, ip, ipinfo, spark)
    }
  }

}
