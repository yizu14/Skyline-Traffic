package com.yi.skyline

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.yi.accumulator.{MonitorAccumulator, MonitorCountAccumulator, SpeedAccumulator}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class blockSpeed(minSpeed: String, normalSpeed: String, medSpeed: String, highSpeed: String, avgSpeed: String)

case class blockSpeedCount4Saving(create_date: String, blockid: String, min_Speed: String, normal_Speed: String, med_Speed: String, high_Speed: String, avg_Speed: String)

case class speedToWeb(create_date:String,avg_speed:String)

object BlockSpeedCount {

  /**
   * 提供数据
   *    avg_speed：平均速度 （分区id，平均速度）>...>（分区id，平均速度）
   *    create_date：创建时间
   */

  var speedToWebString:String = new String
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.default.parallelism", "1")
    val session = SparkSession.builder().appName("BlockSpeedCount")
      .enableHiveSupport()
      .master("local[2]")
      .config(conf)
      .getOrCreate()
    import session.implicits._
    val sc = session.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    val acc = new MonitorAccumulator
    val accc = new MonitorCountAccumulator
    val avgSpeedAcc = new SpeedAccumulator
    sc.register(acc, "monitorAcc")
    sc.register(accc, "monitorCountAcc")
    sc.register(avgSpeedAcc, "SpeedAccumulator")
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "password123")
    val JDBC_URL = "jdbc:mysql://127.0.0.1:3306/traffic"


    /**
     * 统计分区速度排名
     * 封存于2020年4月6日
     * Done
     * 今日记事：Prime Minister Boris Johnson was sent to the hospital due to the COVID-19
     */
    //2018-04-26	0005	43356	深W61995	2018-04-26 02:13:57	174	32	05

    val speedCount = sc.textFile("./monitor_flow_action")
      .map(data => {
        val dataArray = data.split("\t")
        (dataArray(1), dataArray(5))
      })
      .groupByKey()
      //(0001,CompactBuffer(44, 26, 13, 31, 8, 177, 86, 140, 112, 77....)
      .map(data => {
        var minSpeed: Int = 0
        var normalSpeed: Int = 0
        var medSpeed: Int = 0
        var highSpeed: Int = 0

        val dataArray = data._2.toString().dropRight(1).drop(14).split(",")
        dataArray.foreach(data => {
          val dataInt = data.trim.toInt
          avgSpeedAcc.add(dataInt)
          if (dataInt > 0 && dataInt < 60) {
            minSpeed += 1
          } else if (dataInt >= 60 && dataInt < 90) {
            normalSpeed += 1
          } else if (dataInt >= 90 && dataInt < 120) {
            medSpeed += 1
          } else if (dataInt >= 120) {
            highSpeed += 1
          }
        })
        val avgSpeed: String = avgSpeedAcc.value.formatted("%.3f").toString
        avgSpeedAcc.reset()
        //println(highSpeed)
        (data._1, blockSpeed(minSpeed.toString, normalSpeed.toString, medSpeed.toString, highSpeed.toString, avgSpeed))
      })
    //.foreach(println)
    //(0001,blockSpeedCount(3151,1512,1553,1063,69.512))
    saveBlockSpeedRank(speedCount)

    /**
     * 保存分区速度排名
     * Notice:试验，向数据库保存全量数据，并不采用take
     * 封存于2020年4月6日
     * Done
     */
    def saveBlockSpeedRank(speedCount: RDD[(String, blockSpeed)]): Unit = {

      //(0001,blockSpeedCount(3151,1512,1553,1063,69.512))
      val zuluTime = LocalDate.now()
      val speedRank = speedCount.map(data => {
        blockSpeedCount4Saving(zuluTime.toString, data._1, data._2.minSpeed, data._2.normalSpeed, data._2.medSpeed, data._2.highSpeed, data._2.avgSpeed)
      })

      speedRank.foreach(data=>{
        speedToWebString = speedToWebString+">"+speedToWeb(data.blockid,data.avg_Speed).toString.drop(10)
      })
      speedToWebString = speedToWebString.drop(1)
      val conf = new SerializeConfig(true)
      speedToWebString = JSON.toJSONString(speedToWeb(zuluTime.toString,speedToWebString), conf)
      val writer = new PrintWriter(new File("/Users/skyline/Desktop/TestData/BlockSpeedCount/speedCount.log"))
      writer.println(speedToWebString)
      writer.close()

      speedRank
        .toDF()
        .createOrReplaceTempView("blockSpeedCount4Saving")

      session.sql("select * from blockSpeedCount4Saving order by avg_Speed DESC")
        .write
        .mode(SaveMode.Append)
        .jdbc(JDBC_URL, "TopN_Speed", properties)
    }
  }

}
