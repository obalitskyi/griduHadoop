package obalitskyi.gridu.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

class Top10ByCategory {
    def run(events: DataFrame, spark: SparkSession): Unit = {
      // SQL
      val table = "top10ByCategorySQL"

      val top10ByCategorySQL = spark.sql("select category, count(*) as sum from events" +
        " group by category order by sum desc limit 10")

      JDBCMysqlWriter.writeDown(top10ByCategorySQL, table)

      // RDD
      val categoryCounted = events.rdd.map(r => (r.getString(3), 1)).reduceByKey(_ + _)
      val top10ByCategoryRDD = categoryCounted.top(10)(Ordering.by(_._2)).sortBy(_._2)(Ordering.Int.reverse)

      val tableRDD = "top10ByCategoryRDD"
      // to use sparks jdbc method
      JDBCMysqlWriter.writeDown(spark.createDataFrame(top10ByCategoryRDD), tableRDD)
    }
}
