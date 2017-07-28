/*
package com.antin.demo.naiveBayesDemo

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Administrator on 2017/5/18.
  */
object TextCatalog2 {

  case class RawDataRecord(category: String, text: String)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TestNaiveBayes02").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    var srcRDD = sc.textFile("F:\\CommonDevelop\\hadoop\\project\\spark\\spark\\src\\main\\scala\\com\\antin\\demo\\naiveBayesDemo\\data\\sougou-train\\")
      .map {
        x =>
          var data = x.split(",")
          RawDataRecord(data(0), data(1))
      }

    //70%作为训练数据，30%作为测试数据
    val trainingDF = srcRDD.filter(x => x.category.toInt < 20).toDF()
    val testDF = srcRDD.filter(x => x.category.toInt > 10).toDF()

    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)

    //计算每个词在文档中的词频TF
    var hashingTF = new HashingTF().setNumFeatures(100).setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)

    //计算每个词的逆文档频率IDF
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

    testDataRdd.foreach(p => println(model.predict(p.features), p.label))

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))


    //统计分类准确率
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println(testaccuracy)

  }

}
*/
