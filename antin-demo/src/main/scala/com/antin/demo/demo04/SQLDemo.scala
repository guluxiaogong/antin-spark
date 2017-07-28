package com.antin.demo.demo04

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SQLDemo")//.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
   // System.setProperty("user.name", "root")

    val personRdd = sc.textFile("hdfs://zoe01:9000/spark/person.txt").map(line => {
      val fields = line.split(" ")
      Person(fields(0).toLong, fields(1), fields(2).toInt)
    })

    import sqlContext.implicits._
    val personDf = personRdd.toDF

    personDf.registerTempTable("t_person")

    //sqlContext.sql("select * from t_person where age >= 18 order by age desc limit 2").show()
    val df = sqlContext.sql("select * from t_person where age >= 18 order by age desc limit 2")

    //将结果以JSON的方式存储到指定位置
    //df.write.json("F:\\CommonDevelop\\hadoop\\testData\\out4")
    df.write.json("hdfs://zoe01:9000/spark/out")
    sc.stop()

  }
}

case class Person(id: Long, name: String, age: Int)