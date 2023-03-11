package com.yi.accumulator

import org.apache.spark.util.AccumulatorV2

class SpeedAccumulator extends AccumulatorV2[Int,Double]{

  private var result:Double = 0
  private var sum:Int = 0

  override def isZero: Boolean = {
    if(result == 0){
      true
    }else false
  }

  override def copy(): AccumulatorV2[Int, Double] = {
    val newAcc = new SpeedAccumulator
    newAcc.result = this.result
    newAcc.sum = this.sum
    newAcc
  }

  override def reset(): Unit = {
    result = 0
    sum = 0
  }

  override def add(v: Int): Unit = {
    sum +=1
    result+=v
  }

  override def merge(other: AccumulatorV2[Int, Double]): Unit = {
    other match {
      case o:SpeedAccumulator =>{
        result+=o.result
        sum += o.sum
      }
    }
  }

  override def value: Double = {
    result/sum
  }
}
