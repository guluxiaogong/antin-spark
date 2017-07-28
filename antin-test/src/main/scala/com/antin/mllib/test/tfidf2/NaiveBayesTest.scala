package com.antin.mllib.test.tfidf2

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by Administrator on 2017/5/17.
  */
object NaiveBayesTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NaiveBayesTest").setMaster("local")
    val sc = new SparkContext(conf)

    //读入数据
    val data = sc.textFile("F:\\CommonDevelop\\hadoop\\project\\spark\\spark\\src\\main\\scala\\com\\antin\\test\\test\\tfidf2\\data\\data.txt")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }
    // 把数据的60%作为训练集，40%作为测试集.
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    //获得训练模型,第一个参数为数据，第二个参数为平滑参数，默认为1，可改
    val model = NaiveBayes.train(training, lambda = 1.0)

    //对模型进行准确度分析
    val predictionAndLabel = test.map(p => {
      //println(p.features)
      (model.predict(p.features), p.label)
    })
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    println("accuracy-->" + accuracy)
    println("Predictionof (0.0, 2.0, 0.0, 1.0):" + model.predict(Vectors.dense(0.0, 2.0, 0.0, 1.0)))
  }
}
