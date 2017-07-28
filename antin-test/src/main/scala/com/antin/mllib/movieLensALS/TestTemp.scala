package com.antin.mllib.movieLensALS

/**
  * Created by Administrator on 2017/6/2.
  */
object TestTemp {
  def main(args: Array[String]) {
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {

      println(rank)
      println(lambda)
      println(numIter)
      println("=========================")
    }
  }
}
