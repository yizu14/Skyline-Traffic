package com.yi.skyline

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.yi.accumulator.{MonitorAccumulator, MonitorCountAccumulator, SpeedAccumulator}
import com.yi.skyline.StreamingSpeedCount.result
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class topNBlock(create_date: String, blockid: String, auto_count: String)


object TopNCount {

  def main(args: Array[String]): Unit = {

    /**
     * 提供数据：
     *      auto_count：车辆数量（数量｜数量｜数量...｜数量）
     *      blockid：分区id（id｜id｜id...｜id）（与auto_count顺序相对应）
     */


    val conf = new SparkConf()
      .set("spark.default.parallelism", "1")
    val session = SparkSession.builder().appName("TopNCount")
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


    //2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02
    //2018-04-26	0005	43356	深W61995	2018-04-26 02:13:57	174	32	05
    val monitorRdd = sc.textFile("monitor_flow_action")
      .map(data=>{
        val dataArray = data.split("\t")
        flowAction(dataArray(0).trim,dataArray(1).trim,dataArray(2).trim,dataArray(3).trim,dataArray(4).trim,dataArray(5).trim,dataArray(6).trim,dataArray(7).trim)
      })
      .map(data=>(data.blockId,data))
      .groupByKey()
      //.foreach(println)
      //(0005,((2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02),(2018-04-26	0005	43356	深W61995	2018-04-26 02:13:57	174	32	05))
      .map(data=>{
        //print(data._2)
        acc.add(data._2)
        val result =  acc.value
        acc.reset()
        (data._1,result)
      })
    saveTopNBlock(monitorRdd,5)



    /**
     * 统计车流量TopN分区
     *
     * @param monitorRdd
     * @param num
     * 封存于2020年4月5日
     * Done
     * 今日记事：
     */
    def saveTopNBlock(monitorRdd: RDD[(String, String)], num: Int): Unit = {
      //(0004 blockid,7049 摄像头个数 |34949,71489,93665,56591,... 有效摄像头编号 |7220 车辆数)
      val zuluDate = LocalDate.now()
      var autoCount: String = new String
      var blockids: String = new String
      var autoCountString: String = new String
      var resutlString: String = new String
      val sortedRDD = monitorRdd.map(data => {
        val dataArray = data._2.split("\\|")
        autoCount = dataArray(2)
        //println(autoCount)
        (data._1, autoCount)
      })
        .sortBy(_._2, false)
      val array = sortedRDD.take(5)
      array.foreach(data => {
        blockids = blockids + data._1 + "|"
        autoCountString = autoCountString + data._2 + "|"
        //println(autoCountString)
      })
      blockids = blockids.dropRight(1)
      autoCountString = autoCountString.dropRight(1)


      val block = Array(zuluDate.toString + "," + blockids + "," + autoCountString)
      val saveTopNRDD = sc.parallelize(block)
        .map(data => {
          val dataArray = data.split(",")
          topNBlock(dataArray(0), dataArray(1), dataArray(2))
        })
      saveTopNRDD.foreach(data=>{
        var result:String = new String
        val conf = new SerializeConfig(true)
        result = JSON.toJSONString(data, conf)
        val writer = new PrintWriter(new File("/Users/skyline/Downloads/bigdata/topNCount.log"))
        writer.println(result)
        writer.close()
      })
      val saveDF = saveTopNRDD.toDF()
        .createOrReplaceTempView("topNBlock")
      session.sql("select * from topNBlock")
        .write
        .mode(SaveMode.Append)
        .jdbc(JDBC_URL, "TopN_Count", properties)
    }
  }
}
