package com.yi.skyline

import java.util.Properties

import com.yi.accumulator.{MonitorAccumulator, MonitorCountAccumulator, SpeedAccumulator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class inboundAuto(inbound_date: String, plate: String, first_inbound_position: String, roadid: String)

object InboundAutoCount {
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
     * Feature:外地车统计
     * 统计外地车数量及首次出现位置
     * 封存日期：2020年04月09日
     * 今日记事：Wuhan -- the most affected city by COVID-19 is unlocked
     */

    def inboundAutoCount(localPlateStart: String): Unit = {
      val inboundDF = sc.textFile("./monitor_flow_action")
        .filter(data => {
          val dataArray = data.split("\t")
          !dataArray(3).startsWith(localPlateStart)
        })
        .map(data => {
          //2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02
          val dataArray = data.split("\t")
          inboundAuto(dataArray(4), dataArray(3).trim, dataArray(1), dataArray(7))
        })
        .toDF()
        .createOrReplaceTempView("inbound")
      session.sql("select * from inbound order by inbound_date")
        .write
        .mode(SaveMode.Append)
        .jdbc(JDBC_URL, "Inbound_Auto", properties)
    }

    inboundAutoCount("黑")

  }

}
