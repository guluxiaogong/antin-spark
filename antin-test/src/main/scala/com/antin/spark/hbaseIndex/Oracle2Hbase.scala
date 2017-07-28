package com.antin.spark.hbaseIndex

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

/**
  * Created by Administrator on 2017/7/26.
  * /usr/hdp/2.6.1.0-129/spark2/bin/spark-submit --class com.antin.spark.hbaseIndex.Oracle2Hbase --master yarn --deploy-mode client /zoesoft/zoeJobJar/antin-test.jar
  */
object Oracle2Hbase {
  def main(args: Array[String]) {

    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val ss = SparkSession
      .builder()
      .appName("Oracle2Hbase")
      //.master("local[*]")
      //.master("yarn")
      .getOrCreate()

    val resultDF = readFromOracle(ss)

    writeToHbase(ss, "hpor:sehr_xman", resultDF)
  }

  def readFromOracle(ss: SparkSession): DataFrame = {
    val jdbcDF = ss.read
      .format("jdbc")
      .option("partitionColumn", "type")
      .option("lowerBound", 1)
      .option("upperBound", 5)
      .option("numPartitions", 2)
      .option("fetchSize", 1000)
      .option("url", "jdbc:oracle:thin:@192.168.0.91:1521:xmhealth")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("dbtable", "SEHR_XMAN_EVENT_20140101")
      .option("user", "sehr_zoe")
      .option("password", "sehr_zoe")
      .load()
    jdbcDF.createOrReplaceTempView("SEHR_XMAN_EVENT")
    ss.sql("select xman_id,icd_code,diagnosis from SEHR_XMAN_EVENT where icd_code is not null and diagnosis is not null")

  }

  def writeToHbase(ss: SparkSession, tableName: String, df: DataFrame): Unit = {
    //import ss.implicits._
    import ss.implicits._
    val conf = HBaseConfiguration.create()
    val jobConf = new JobConf(conf)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val dff = df.filter(row => {
      val icdCode = row.getAs[String]("icd_code").trim
      if ("".equals(icdCode))
        false
      else
        true
    })

    val rs = dff.map { row =>
      var diagnosis = row.getAs[String]("diagnosis")
      if (diagnosis == null || "".equals(diagnosis.trim))
        diagnosis = ""
      (row.getAs[String]("xman_id"),
        "diagnose",
        row.getAs[String]("icd_code"),
        diagnosis
        )
    }
    val rss = rs.rdd.map { values =>
      val put = new Put(Bytes.toBytes(values._1))
      put.addColumn(Bytes.toBytes(values._2), Bytes.toBytes(values._3), Bytes.toBytes(values._4))
      (new ImmutableBytesWritable, put)
    }

    rss.saveAsHadoopDataset(jobConf)
  }

}
