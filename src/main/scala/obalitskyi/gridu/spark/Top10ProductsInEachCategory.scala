package obalitskyi.gridu.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Top10ProductsInEachCategory {
  def run(events: DataFrame, spark: SparkSession): Unit = {
    // SQL
    val tableSQL = "top10ProductsInEachCategorySQL"
    val top10ProductsInEachCategorySQL = spark.sql("select product, category, frequency from" +
      " (select product, category, frequency, row_number() over (partition by category order by frequency desc) as rank from" +
      " (select product, category, count(*) as frequency from events group by product, category) ev) ranked where ranked.rank <= 10" +
      " order by category, frequency desc")

    JDBCMysqlWriter.writeDown(top10ProductsInEachCategorySQL, tableSQL)

    // DF
    val tableDF = "top10ProductsInEachCategoryDF"
    val groupedAndCounted = events
      .groupBy("product", "category")
      .count()
      .withColumnRenamed("count", "frequency")

    val ranked = groupedAndCounted
      .select(col("product"), col("category"), col("frequency"),
        row_number().over(Window.partitionBy("category").orderBy(desc("frequency"))).alias("rank"))

    val top10ProductsInEachCategoryDF = ranked.select("product", "category", "frequency")
      .where("rank <= 10")
      .orderBy(col("category"), desc("frequency"))

    JDBCMysqlWriter.writeDown(top10ProductsInEachCategoryDF, tableDF)


    // RDD
    val tableRDD = "top10ProductsInEachCategoryRDD"
    val prodAndCategoryCounted = events.rdd.map(r => ((r.getString(0), r.getString(3)), 1L)).reduceByKey(_ + _)

    val keyedByCategory = prodAndCategoryCounted.map(r => (r._1._2, (r._1._1, r._2)))

    val zeroVal = new BoundedPriorityQueue[(String, Long)](10)(Ordering.by(_._2))
    val agg = (q: BoundedPriorityQueue[(String, Long)], el: (String, Long)) => {
      q += el
    }
    val merge = (l1: BoundedPriorityQueue[(String, Long)], l2: BoundedPriorityQueue[(String, Long)]) => {
      l1 ++= l2
    }

    val top10ProductsInEachCategoryRDD = keyedByCategory.aggregateByKey(zeroVal)(agg, merge).flatMapValues(_.iterator)
      .map(e => (e._1, e._2._1, e._2._2))
      .sortBy(e => (e._1, -e._3))

    JDBCMysqlWriter.writeDown(spark.createDataFrame(top10ProductsInEachCategoryRDD), tableRDD)
  }
}
