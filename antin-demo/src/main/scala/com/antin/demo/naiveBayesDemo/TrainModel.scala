/*
package com.antin.demo.naiveBayesDemo

import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2017/5/18.
  */
object TrainModel {

  case class RawDataRecord(category: String, text: String)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TrainModel").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //加载数据
    var srcRDD = sc.textFile("F:\\CommonDevelop\\hadoop\\project\\spark\\spark\\src\\main\\scala\\com\\antin\\demo\\naiveBayesDemo\\data\\sougou-train\\")
      .map {
        x =>
          var data = x.split(",")
          RawDataRecord(data(0), data(1))
      }
    //将数据转化成DataFrame
    var trainingDF = srcRDD.toDF()

    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)

    //计算每个词在文档中的词频TF
    var hashingTF = new HashingTF().setNumFeatures(100).setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)

    //计算每个词的IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)

    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //训练模型
    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

    model.save(sc, "F:\\CommonDevelop\\hadoop\\project\\spark\\spark\\src\\main\\scala\\com\\antin\\demo\\naiveBayesDemo\\data\\model\\")
    // val sameModel = NaiveBayesModel.load(sc, "target/tmp/naiveBayesModel")

  }
}
*/
