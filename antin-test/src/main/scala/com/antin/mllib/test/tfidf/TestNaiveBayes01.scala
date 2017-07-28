package com.antin.mllib.test.tfidf

import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2017/5/17.
  */
object TestNaiveBayes01 {

  case class RawDataRecord(category: String, text: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TestNaiveBayes01").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
    var srcDF = sc.textFile("F:\\CommonDevelop\\hadoop\\project\\spark\\spark\\src\\main\\scala\\com\\antin\\test\\test\\tfidf\\data\\test1.txt").map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0), data(1))
    }.toDF()

    srcDF.select("category", "text").take(2).foreach(println)
    //[0,苹果 官网 苹果 宣布]
    //[1,苹果 梨 香蕉]

    //将分好的词转换为数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(srcDF)

    wordsData.select($"category", $"text", $"words").take(2).foreach(println)
    //[ 0, 苹果 官网 苹果 宣布, WrappedArray(苹果, 官网, 苹果, 宣布)]
    //[1, 苹果 梨 香蕉, WrappedArray(苹果, 梨, 香蕉)]

    //将每个词转换成Int型，并计算其在文档中的词频（TF）
    var hashingTF =
      new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
    var featurizedData = hashingTF.transform(wordsData)

    featurizedData.select($"category", $"words", $"rawFeatures").take(2).foreach(println)
    // 结果中，“苹果”用23来表示，第一个文档中，词频为2，第二个文档中词频为1.
    //[0,WrappedArray(苹果, 官网, 苹果, 宣布),(100,[23,81,96],[2.0,1.0,1.0])]
    //[1,WrappedArray(苹果, 梨, 香蕉),(100,[23,72,92],[1.0,1.0,1.0])]


    //计算TF-IDF值
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    rescaledData.select($"category", $"words", $"features").take(2).foreach(println)
    //因为一共只有两个文档，且都出现了“苹果”，因此该词的TF-IDF值为0.
    //[0,WrappedArray(苹果, 官网, 苹果, 宣布),(100,[23,81,96],[0.0,0.4054651081081644,0.4054651081081644])]
    //[1,WrappedArray(苹果, 梨, 香蕉),(100,[23,72,92],[0.0,0.4054651081081644,0.4054651081081644])]

    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    trainDataRdd.take(1)
    //trainDataRdd.foreach(println)
  }


}
