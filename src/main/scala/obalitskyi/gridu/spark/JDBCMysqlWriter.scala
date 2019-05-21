package obalitskyi.gridu.spark

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode}

// hardcoded
// will fix in next commit
object JDBCMysqlWriter {
  Class.forName("com.mysql.jdbc.Driver")
  val url = "jdbc:mysql://ip-10-0-0-21.us-west-1.compute.internal:3306/obalitskyi"
  val props = new Properties()
  props.put("user", "obalitskyi")
  props.put("driver", "com.mysql.jdbc.Driver")
  val mode = SaveMode.Overwrite

  def writeDown(df: DataFrame, table: String): Unit = {
    df.write.mode(mode).jdbc(url, table, props)
  }
}
