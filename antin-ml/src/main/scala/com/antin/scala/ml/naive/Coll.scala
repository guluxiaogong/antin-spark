package com.antin.scala.ml.naive

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Set

/**
  * Created by Administrator on 2017/8/7.
  */
object Coll {

  //设置环境变量
  val conf = new SparkConf().setMaster("local").setAppName(this.getClass().getSimpleName().filter(!_.equals('$')))

  println(this.getClass().getSimpleName().filter(!_.equals('$')))
  //实例化环境
  val sc = new SparkContext(conf)
  val users = Set[String]()

  var source = Map[String, Map[String, Double]]()

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("NaiveBayesExample")
      .master("local[*]")
      .getOrCreate()
    val data = spark.read.textFile("D:\\ZHS\\智能预约\\data\\features-1.txt")
    //import spark.implicits._
    //validate(spark, data, "0405-2-1021310-88359")
    getSource(spark) //初始化
    val name = "0405-2-1021310-88359" //设定目标对象

    //打印用
    //    users.filter(_.startsWith(name.split("-")(0))).map(user => {
    //      //迭代进行计算
    //      ("2 乳腺外科门诊 胸外科专业 主任医师 " + " 相对于 " + validate(spark, data, user) + "的相似性分数是：", getCollaborateSource(name, user))
    //    }).toList.sortWith(_._2 > _._2).foreach(x => {
    //      println(x._1 + x._2)
    //    })

    //正试输出用
    val result = users.map(u => {
      val user2users = users.filter(_.startsWith(u.split("-")(0))).map(user => {
        (u, user, getCollaborateSource(name, user))
      }).toList.sortWith(_._3 > _._3).map(x => {
        (x._1.split("-").tail.mkString("-") + " ", x._2.split("-").tail.mkString("-"))
      }).groupBy(_._1).values.head.map(x => {
        x._2
      })
      //      u + " " + user2users.mkString(",")
      val uu = u.split("-").tail.mkString("-")
      uu + " " + user2users.dropWhile(_ == uu).mkString(",")
      //  println(u + " " + user2users.mkString(","))
    })
    val resultRdd = sc.parallelize(result.toSeq)
    resultRdd.saveAsTextFile("D:\\ZHS\\智能预约\\data\\医生与医生们关系")
    println()
    //println("2|1021310|88359" + " 相对于 " + "7|321|1521" + "的相似性分数是：" + getCollaborateSource("2|1021310|88359", "3|1190002|8146"))
    spark.stop()
  }


  def validate(spark: SparkSession, data: Dataset[String], name: String): String = {
    import spark.implicits._
    val value = data.select("value").map(x => {
      val v = x.getAs[String]("value").split(" ")
      (v(0), v.tail)
    }).filter(_._1 == name).map(_._2)
    value.first().mkString(" ")
  }

  def getSource(spark: SparkSession): Map[String, Map[String, Double]] = {
    //    val spark = SparkSession
    //      .builder
    //      .appName("NaiveBayesExample")
    //      .master("local[*]")
    //      .getOrCreate()
    // implicit spark.implicits._

    val data = spark.read.textFile("D:\\ZHS\\智能预约\\data\\features-2\\*.txt")
    //println(data.collect().tail(2) + "=================>" + data.collect().length)
    data.foreach(x => {
      val lines = x.split(" ")
      users.add(lines(0))
      source += (lines(0) -> Map("org_id" -> lines(2).trim.toDouble, "name" -> lines(3).trim.toDouble, "s_name" -> lines(4).trim.toDouble, "tech_title" -> lines(5).trim.toDouble))
    })
    source
  }

  //两两计算,采用余弦相似性
  def getCollaborateSource(user1: String, user2: String): Double = {
    val user1FilmSource = source.get(user1).get.values.toVector //获得第1个用户的评分
    val user2FilmSource = source.get(user2).get.values.toVector //获得第2个用户的评分
    val member = user1FilmSource.zip(user2FilmSource).map(d => d._1 * d._2).reduce(_ + _).toDouble //对公式分子部分进行计算
    val temp1 = math.sqrt(user1FilmSource.map(num => {
        //求出分母第1个变量值
        math.pow(num, 2) //数学计算
      }).reduce(_ + _)) //进行叠加
    //println("temp1:" + temp1)
    val temp2 = math.sqrt(user2FilmSource.map(num => {
        ////求出分母第2个变量值
        math.pow(num, 2) //数学计算
      }).reduce(_ + _)) //进行叠加
    val denominator = temp1 * temp2 //求出分母
    member / denominator //进行计算
  }
}
