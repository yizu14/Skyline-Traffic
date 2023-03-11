package com.yi.accumulator

import com.yi.skyline.flowAction
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.Set

class MonitorAccumulator extends AccumulatorV2[Iterable[flowAction],String]{

  private var result: String = new String
  private var moniterSet: Set[String] = Set()
  private var moniterString:String = new String
  private var autoCount:Set[String] = Set()


  override def isZero: Boolean = {
    result.isEmpty
  }


  override def reset(): Unit = {
    //result = ""

    moniterString=""
    moniterSet.clear()

  }

  override def add(v: Iterable[flowAction]): Unit = {
    //((2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02),(2018-04-26	0005	43356	深W61995	2018-04-26 02:13:57	174	32	05))
    v.foreach(data=>{

      //moniterString+=data.monitorId+","
      moniterSet.add(data.monitorId)
      autoCount.add(data.plateNum)
      //moniterSet.foreach(println)
    })
  }

  override def merge(other: AccumulatorV2[Iterable[flowAction], String]): Unit = {
    other match {
      case o:MonitorAccumulator =>{

      }
    }
  }

  override def value: String = {
    //print(moniterString)
    moniterSet.foreach(data=>{
      //print(data)
      moniterString+=data+","
    })
    moniterString = moniterString.dropRight(1)
    result = moniterSet.size+"|"+moniterString+"|"+autoCount.size
    result
  }

  override def copy(): AccumulatorV2[Iterable[flowAction], String] = {
    val newAcc = new MonitorAccumulator
    newAcc.moniterSet = this.moniterSet
    newAcc.moniterString = this.moniterString
    newAcc.autoCount = this.autoCount
    newAcc
  }
}
