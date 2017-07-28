package com.antin.demo.naiveBayesTest

import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2017/5/18.
  * 测试数据
  * 资料：http://note.youdao.com/noteshare?id=3e248dc9695df2f2ff9208113933cb56&sub=FC6CBFD3B644452CB0A069B65B892FCC
  */
object TestForecast {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TrainModel").setMaster("local")
    val sc = new SparkContext(conf)
    val model = NaiveBayesModel.load(sc, "F:\\CommonDevelop\\hadoop\\project\\spark\\spark\\src\\main\\scala\\com\\antin\\demo\\naiveBayesTest\\data\\model")
    println("Predictionof (0.0, 2.0, 0.0, 1.0):" + model.predict(Vectors.dense(2.0, 2.0, 0.0, 1.0)))
  }

}
