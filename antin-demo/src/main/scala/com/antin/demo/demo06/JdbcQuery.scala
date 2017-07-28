package com.antin.demo.demo06

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/7/17.
  */
object JdbcQuery {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("JdbcRDDQuery")
      .master("local[*]")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@192.168.0.91:1521:xmhealth")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", "A_TEST")
      .option("user", "sehr_zoe")
      .option("password", "sehr_zoe")
      .load()
    //jdbcDF.select("id", "name", "sex").write.format("parquet").save("src/main/resources/sehr_xman")
   // jdbcDF.select("id", "name", "sex").write.format("json").save("src/main/resources/sec_users")

    //存储成为一张虚表user_abel
    //    jdbcDF.select("id", "name", "sex").write.mode("overwrite").saveAsTable("user_abel")
    //    val jdbcSQl = spark.sql("select * from SEHR_XMAN where name like '王%' ")
    //    jdbcSQl.show()
    //    jdbcSQl.write.format("json").save("./out/resulted")

    jdbcDF.select("code", "value").write.mode("overwrite").saveAsTable("A_TEST")
    val jdbcSQl = spark.sql("select * from A_TEST")
    jdbcSQl.show()

  }


}
