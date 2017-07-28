package com.antin.demo.naiveBayesTest

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2017/5/18.
  * 对模型进行训练
  */
object TrainModel {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NaiveBayesTest").setMaster("local")
    val sc = new SparkContext(conf)

    //读入数据
    val data = sc.textFile("F:\\CommonDevelop\\hadoop\\project\\spark\\spark\\src\\main\\scala\\com\\antin\\demo\\naiveBayesTest\\data\\data.txt")
    val training = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    //获得训练模型,第一个参数为数据，第二个参数为平滑参数，默认为1，可改
    val model = NaiveBayes.train(training, lambda = 1.0)
    model.save(sc, "F:\\CommonDevelop\\hadoop\\project\\spark\\spark\\src\\main\\scala\\com\\antin\\demo\\naiveBayesTest\\data\\model")

  }
}
