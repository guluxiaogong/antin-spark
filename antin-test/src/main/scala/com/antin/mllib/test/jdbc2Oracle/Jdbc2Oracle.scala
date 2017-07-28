package com.antin.mllib.test.jdbc2Oracle

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/8.
  */
object Jdbc2Oracle {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]") //.setMaster("spark://zoe01:7077")//
    val sc = new SparkContext(conf)
    System.setProperty("user.name", "root")
    System.setProperty("HADOOP_USER_NAME", "root")

    val connection = () => {
      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
      DriverManager.getConnection("jdbc:oracle:thin:@192.168.0.91:1521:xmhealth", "sehr", "sehr")
    }
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "select * from ( select rownum as id,xman_id, event, catalog_code, serial, content, t.xml.getclobval() xml, compression, encryption, status, version, title, commit_time, istemp from SEHR_XMAN_EHR_0 t) where id>=? and id<=?",
      1, 1, 1,
      r => {
        val id = r.getInt(1)
        val xmanId = r.getString(2)
        val event = r.getString(3)
        val content = r.getBlob("content")
        val xml = r.getString("xml")
        // al blob = content.asInstanceOf[BLOB].getLocator
        // (id, code, blob, xml)
        //(id, xmanId, event)

        id + "," + xmanId + "," + event
      }
    )
    // println(jdbcRDD.collect().toBuffer)
    //jdbcRDD.saveAsTextFile("hdfs://zoe01:9000/spark2/out")

    //    val rdd = jdbcRDD.map(x => {
    //
    //      // println(x._1 + "," + x._2 + "," + x._3)
    //      x._1 + "," + x._2 + "," + x._3
    //    })
    //println(rdd.collect.toBuffer)
    jdbcRDD.saveAsTextFile("F:\\CommonDevelop\\hadoop\\testData\\out3")
    sc.stop()
  }

  def testJdbc() = {
    //    val url = "jdbc:mysql://mysqlHost:3306/database"
    //    val tableName = "table"
    //
    //    // 设置连接用户&密码
    //    val prop = new java.util.Properties
    //    prop.setProperty("user","username")
    //    prop.setProperty("password","pwd")
    //
    //    // 取得该表数据
    //    val jdbcDF = sqlContext.read.jdbc(url,tableName,prop)
  }

  def jdbc() = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "select xman_id, event, catalog_code, serial, content, t.xml.getclobval() xml, compression, encryption, status, version, title, commit_time, istemp from SEHR_XMAN_EHR_0 t where event='0d0c000f-300d-1d12-b123-a8d3c3b12345'"
    try {
      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
      conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.0.91:1521:xmhealth", "sehr", "sehr")
      ps = conn.prepareStatement(sql)
      ps.executeQuery()
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }
}
