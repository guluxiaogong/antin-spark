/*
package com.antin.mllib.test.tfidf

/**
  * Created by Administrator on 2017/5/17.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{SQLContext, Row}

object TestNaiveBayes02 {


  case class RawDataRecord(category: String, text: String)

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("TestNaiveBayes02").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    var srcRDD = sc.textFile("F:/CommonDevelop/hadoop/project/spark/spark/src/main/scala/com/antin/test/test/tfidf/data/sougou-train-test/")
      .map {
        x =>
          var data = x.split(",")
          RawDataRecord(data(0), data(1))
      }
    srcRDD.take(3).foreach(println)

    //70%作为训练数据，30%作为测试数据
    val splits = srcRDD.randomSplit(Array(0.7, 0.3))
    var trainingDF = splits(0).toDF()
    var testDF = splits(1).toDF()

    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)
    println("output1：")
    wordsData.select($"category", $"text", $"words").take(1).foreach(println)

    //计算每个词在文档中的词频TF
    var hashingTF = new HashingTF().setNumFeatures(100).setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)
    println("output2：")
    featurizedData.select($"category", $"words", $"rawFeatures").take(1).foreach(println)

    //计算每个词的逆文档频率IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)
    println("output3：")
    rescaledData.select($"category", $"features").take(1).foreach(println) //[0,(100,[4,58,60,65],[1.3862943611198906,1.3862943611198906,1.3862943611198906,1.3862943611198906])]

    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
        //println("**************************")
        //println(features.toArray.toBuffer)//ArrayBuffer(0.0, 0.0, 0.0, 0.0, 1.3862943611198906, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.3862943611198906, 0.0, 1.3862943611198906, 0.0, 0.0, 0.0, 0.0, 1.3862943611198906, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    println("output4：")
    // trainDataRdd.take(1).foreach(println)
    //trainDataRdd.saveAsTextFile("F:/CommonDevelop/hadoop/project/spark/spark/src/main/scala/com/antin/test/test/tfidf/data/out")

    //训练模型
    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

    //model.save(sc, "target/tmp/naiveBayesModel")
    // val sameModel = NaiveBayesModel.load(sc, "target/tmp/naiveBayesModel")

    //测试数据集，做同样的特征表示及格式转换
    var testwordsData = tokenizer.transform(testDF)
    var testfeaturizedData = hashingTF.transform(testwordsData)
    var testrescaledData = idfModel.transform(testfeaturizedData)
    var testDataRdd = testrescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))

    //统计分类准确率
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("output5：")
    println(testaccuracy)
    println("Predictionof: " + model.predict(Vectors.dense(0.0, 0.0, 0.0, 0.0, 1.3862943611198906, 0.0, 1.2568, 0.0, 0.0, 0.0, 1.15789, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.3862943611198356, 0.0, 0.0, 0.0, 0.0, 1.3862943611198906, 0.0, 1.3862943611198906, 0.0, 0.0, 0.0, 0.0, 1.3862943611198906, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))) //Array(0.0, 2.0, 0.0, 1.0,3)数值为将每个词转换成Int型，输出这些词的分类

  }
}*/
