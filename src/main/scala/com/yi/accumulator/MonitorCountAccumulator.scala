package com.yi.accumulator

import com.yi.skyline.blockCamera
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.Set

class MonitorCountAccumulator extends AccumulatorV2[Iterable[String],String]{

  private var result: String = new String
  private var moniterSet: Set[String] = Set()
  private var moniterString:String = new String

  override def isZero: Boolean = {
    result.isEmpty
  }

  override def copy(): AccumulatorV2[Iterable[String], String] = {
    val newAcc = new MonitorCountAccumulator
    newAcc.moniterSet = this.moniterSet
    newAcc.moniterString = this.moniterString
    newAcc
  }

  override def reset(): Unit = {

    moniterString=""
    moniterSet.clear()
  }

  override def add(v: Iterable[String]): Unit = {
    v.foreach(data=>{
      moniterSet.add(data)
    })
  }

  override def merge(other: AccumulatorV2[Iterable[String], String]): Unit = {
    other match{
      case o:MonitorCountAccumulator =>{

      }
    }
  }

  override def value: String = {
    moniterSet.foreach(data=>{
      moniterString+=data+","
    })
    moniterString = moniterString.dropRight(1)
    result = moniterSet.size+"|"+moniterString
    result
  }
}
