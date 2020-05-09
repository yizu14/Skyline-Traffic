package com.yi.accumulator

import org.apache.spark.util.AccumulatorV2


//2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02
class CurrentAutoAccumulator extends AccumulatorV2[String,(String,String)]{
  private var currentAutoCount:Int = 0
  private var currentInboundAutoCount:Int = 0
  override def isZero: Boolean = {
    if(currentAutoCount==0&&currentInboundAutoCount==0){
      true
    } else {
      false
    }
  }

  override def copy(): AccumulatorV2[String, (String,String)] = {
    val newAcc = new CurrentAutoAccumulator
    newAcc.currentAutoCount = this.currentAutoCount
    newAcc.currentInboundAutoCount = this.currentInboundAutoCount
    newAcc
  }

  override def reset(): Unit = {

  }

  override def add(v: String): Unit = {
    val dataArray = v.split("\t")
    if(!dataArray(3).startsWith("黑A")){
      currentInboundAutoCount+=1
    }
    currentAutoCount+=1
    print("当前CAC:")
    println(currentAutoCount)
  }

  override def merge(other: AccumulatorV2[String, (String,String)]): Unit = {
    other match {
      case o:CurrentAutoAccumulator =>{
        currentAutoCount+=o.currentAutoCount
        currentInboundAutoCount+=o.currentInboundAutoCount
      }
    }
  }

  override def value: (String,String) = {
    (currentAutoCount.toString,currentInboundAutoCount.toString)
  }
}
