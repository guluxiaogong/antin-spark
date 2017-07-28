package com.antin.mllib.test.tfidf3

import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/5/18.
  */
object NaiveBayesTest3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NaiveBayesTest3").setMaster("local")
    val sc = new SparkContext(conf)

    val data = sc.textFile("C:\\Users\\alienware\\IdeaProjects\\sparkCore\\data\\mllib\\sample_naive_bayes_data.txt")//没有测试数据
    val parseData = data.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))

    }

    // Split the data into training and test sets (50% held out for testing)
    val splitData = parseData.randomSplit(Array(0.5, 0.5), seed = 1L)
    val trainData = splitData(0)
    val testData = splitData(1)

    // Train  naiveBayesModel
    val model = NaiveBayes.train(trainData, lambda = 1.0, modelType = "multinomial")

    val labelsAndPredictions = testData.map(p => (model.predict(p.features), p.label))

    labelsAndPredictions.foreach(println)
    /**
      * (0.0,0.0)
      * (1.0,1.0)
      * (1.0,1.0)
      * (1.0,1.0)
      * (2.0,2.0)
      * (2.0,2.0)
      */

    val accuracy = labelsAndPredictions.filter(p => p._1 == p._2).count() / testData.count()
    println("精准度：" + accuracy)
    //精准度：1

    // Save and load model
    model.save(sc, "target/tmp/naiveBayesModel")
    val sameModel = NaiveBayesModel.load(sc, "target/tmp/naiveBayesModel")

  }
}
