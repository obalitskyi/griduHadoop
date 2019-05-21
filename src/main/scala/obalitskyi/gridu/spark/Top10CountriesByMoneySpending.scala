package obalitskyi.gridu.spark


import org.apache.spark.sql.{DataFrame, SparkSession}

class Top10CountriesByMoneySpending {
  def run(events: DataFrame, ip: DataFrame, ipinfo: DataFrame, spark: SparkSession): Unit = {
    // SQL
    val tableSQL = "top10CountriesByMoneySpendingSQL"
    val top10CountriesByMoneySpendingSQL = spark.sql("select country_name, sum(price) as sum from " +
      " events e join ip ipp on ipp.network = e.ip" +
      " join ipinfo f on ipp.geoname_id = f.geoname_id" +
      " group by country_name" +
      " having country_name != 'null'" +
      " order by sum desc limit 10")

    JDBCMysqlWriter.writeDown(top10CountriesByMoneySpendingSQL, tableSQL)

    // RDD
    val ipB = spark.sparkContext.broadcast(ip.rdd.map(r => (r.getString(0), r.getString(1)))
      .filter(t => t._1 != null && t._2 != null).collectAsMap())
    val ipinfoB = spark.sparkContext.broadcast(ipinfo.rdd.map(r => (r.getString(0), r.getString(5)))
      .filter(t => t._1 != null && t._2 != null).collectAsMap())

    val countryPrice = events.rdd.map(r => {
      val net = r.getString(4)
      val geoId = ipB.value.getOrElse(net, "")
      val country = ipinfoB.value.getOrElse(geoId, "")

      (country, r.getDouble(1))
    }).filter(_._1.nonEmpty)

    val countryTotalSum = countryPrice.reduceByKey(_ + _)

    val top10CountriesByMoneySpending = countryTotalSum.top(10)(Ordering.by(_._2))

    val top10CountriesByMoneySpendingRDD = spark.createDataFrame(top10CountriesByMoneySpending)

    top10CountriesByMoneySpendingRDD.coalesce(1)

    val tableRDD = "top10CountriesByMoneySpendingRDD"
    // in order to use jdbc method
    JDBCMysqlWriter.writeDown(top10CountriesByMoneySpendingRDD, tableRDD)
  }
}
