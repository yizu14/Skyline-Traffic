package com.yi.skyline

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.yi.accumulator.{MonitorAccumulator, MonitorCountAccumulator, SpeedAccumulator}
import com.yi.skyline.StreamingSpeedCount.result
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.Set
import scala.collection.mutable.HashMap

case class flowAction(createDate:String, blockId:String, monitorId:String, plateNum:String, actionTime:String,speed:String,roadId:String,areaId:String)
case class blockCamera(blockId:String,monitorId:String)
case class monitorState(create_date:String,blockid:String,monitor_standard_count:String,normal_monitor_count:String,abnormal_monitor_count:String,abnormal_monitor_camera_infos:String)
case class monitorToWebSave(create_date:String,blockMonitor:String)
case class monitorList(blockid:String,brokenList:String)
case class monitorToRayData(blockid:String,brokenCount:String,monitorSum:String)
//case class blockSpeedCount(minSpeed:String,normalSpeed:String,medSpeed:String,highSpeed:String,avgSpeed:String)
//case class blockSpeedCount4Saving(create_date:String,blockid:String,min_Speed:String,normal_Speed:String,med_Speed:String,high_Speed:String,avg_Speed:String)
//case class autoTrack(create_date:String,plate:String,blockid:String,roadid:String,districtid:String)
//case class inboundAuto(inbound_date:String, plate:String,first_inbound_position:String,roadid:String)
//case class streamSpeed(create_date:String,plate:String,speed:String)

/**
 * 开发日志：
 * Feature1：检测分区状态  生成monitorRDD，blockMonitor，brokenMonitor，采用saveMonitorState函数，影响monitor_state表
 * Feature2：统计分区车辆数排名
 * Feature3：统计分区速度排名
 *
 */



object MonitorFlowAnalyze {

  var result: String = new String
  var standardRayData:String = new String
  var brokenRayData:String = new String
  var blockRayData:String = new String

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.default.parallelism", "1")
    val session = SparkSession.builder().appName("Monitor")
      .enableHiveSupport()
      .master("local[2]")
      .config(conf)
      .getOrCreate()
    import session.implicits._
    val sc = session.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    //val sc = new SparkContext(new SparkConf().setAppName("monitor").setMaster("local"))
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
         * 统计摄像头状态
         * 封存于2020年4月4日
         * Done
         * 今日记事：清明节，国家公祭日，向抗击疫情的英雄致敬
         *          Tomb Sweeping Day, National Memorial Day, Pray for the HERO, Thank you. R.I.P
         */

    /**
     * 提供数据：
     *      blockMonitor：分区损坏摄像头列表（分区id，数量｜摄像头id，摄像头id...摄像头id）
     *      create_date：创建时间
     */

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
          //.foreach(println)
          //(0004 blockid,7049 摄像头个数 |34949,71489,93665,56591,... 有效摄像头编号 |7220 车辆数)



        //0005	71892
        val blockmonitor = sc.textFile("monitor_camera_info")
          .map(data=>{
            val dataArray = data.split("\t")
            blockCamera(dataArray(0).trim,dataArray(1).trim)
          })
          .map(data=>(data.blockId,data.monitorId))
          .groupByKey()
          //.foreach(println)
          .map(data=>{
            accc.add(data._2)
            val result = accc.value
            accc.reset()
            (data._1,result)
          })
          //.foreach(println)
          //(0004 blockid,7057 标准摄像头个数|34949,71489,93665,...摄像头列表)

        //Feature1:统计损坏的摄像头
        val brokenMonitor = blockmonitor.leftOuterJoin(monitorRdd)
          //.foreach(println)
          .map(data=>{
            val onlineMonitor: Set[String] = Set()
            val standardMonitor:Set[String] = Set()
            var offlineMonitor: Set[String] = Set()
            val dataArray = data._2._1.split("\\|")
            val standardMonitorList = dataArray(1).split(",")
            val onlineMonitorData = data._2._2.toString.split("\\|")
            val onlineMonitorList = onlineMonitorData(1).split(",")
            var offlineMonitorString: String = new String
            var offlineMonitorCount: Int = 0
            var result:String =  new String

            standardMonitorList.foreach(data=>{
              standardMonitor.add(data)
            })
            onlineMonitorList.foreach(data=>{
              onlineMonitor.add(data)
            })
            offlineMonitor = standardMonitor--onlineMonitor
            offlineMonitor.foreach(data=>{
              offlineMonitorString = offlineMonitorString + data +","
            })
            offlineMonitorString = offlineMonitorString.dropRight(1)
            offlineMonitorCount = offlineMonitor.size
            result = standardMonitor.size+"|"+onlineMonitor.size+"|"+offlineMonitorCount+"|"+offlineMonitorString
            (data._1,result)
          })
        //  .foreach(println)
        //(0004, Set(78793, 64257, 69020, 84806, 42685, 64092, 92818, 26268))


        saveMonitorState(brokenMonitor)
        saveToWebDisplay(brokenMonitor)
        saveToRayData(brokenMonitor)
//        saveTopNBlock(monitorRdd,5)



    def saveToWebDisplay(brokenMonitor:RDD[(String,String)]):Unit = {

      val zuluDate = LocalDate.now()
      brokenMonitor.foreach(data=>{
        val dataArray = data._2.split("\\|")
        val strings = dataArray(2)+"|"+dataArray(3)
        val temp = (data._1,strings)
        result= result + ">" + temp.toString()
      })
      result = result.drop(1)
      //val dataArray = result.split(">")
      val conf = new SerializeConfig(true)
      result = JSON.toJSONString(monitorToWebSave(zuluDate.toString,result), conf)
      val writer = new PrintWriter(new File("/Users/skyline/Desktop/TestData/MonitorFlowAnalyze/monitorFlow.log"))
      writer.println(result)
      writer.close()
      result = ""
    }


        //理想需求：(blockid,normal_monitor_count|abnormal_monitor_count|abnormal_monitor_infos)
        def saveMonitorState(brokenMonitor:RDD[(String,String)]):Unit = {
          //monitorState(create_date:String,blockid:String,normal_monitor_count:String,normal_camera_count:String,abnormal_monitor_count:String,abnormal_camera_count:String,abnormal_monitor_camera_infos:String)
          val zuluDate = LocalDate.now()
          val brokenMonitorRDD = brokenMonitor.map(data=>{
            val dataArray = data._2.split("\\|")
            monitorState(zuluDate.toString,data._1,dataArray(0).trim,dataArray(1).trim,dataArray(2).trim,dataArray(3).trim)
          })
          val monitorStateDF = brokenMonitorRDD.toDF()
          monitorStateDF.createOrReplaceTempView("monitorState")
          val saveDF = session.sql("select * from monitorState order by create_date ASC, blockid ASC")
          saveDF.show()

          saveDF.write.mode(SaveMode.Append).jdbc(JDBC_URL,"monitor_state",properties)
        }

    def saveToRayData(brokenMonitor:RDD[(String,String)]):Unit = {
      //(0004, Set(78793, 64257, 69020, 84806, 42685, 64092, 92818, 26268),7052)

      val zuluDate = LocalDate.now()
      brokenMonitor.foreach(data=>{
        val dataArray = data._2.split("\\|")
        standardRayData = standardRayData+dataArray(0)+"|"
        brokenRayData = brokenRayData+dataArray(2)+"|"
        blockRayData = blockRayData+data._1+"|"
      })
      val save = monitorToRayData(blockRayData.dropRight(1),standardRayData.dropRight(1),brokenRayData.dropRight(1))
      val conf = new SerializeConfig(true)
      result = JSON.toJSONString(save, conf)
      val writer = new PrintWriter(new File("/Users/skyline/Desktop/TestData/MonitorFlowAnalyze/monitorFlowRay.log"))
      writer.println(result)
      writer.close()
      result = ""
    }


    //
    //    /**
    //     * 统计车流量TopN分区
    //     * @param monitorRdd
    //     * @param num
    //     * 封存于2020年4月5日
    //     * Done
    //     * 今日记事：
    //     */
    //    def saveTopNBlock(monitorRdd:RDD[(String,String)],num:Int):Unit = {
    //      //(0004 blockid,7049 摄像头个数 |34949,71489,93665,56591,... 有效摄像头编号 |7220 车辆数)
    //      val zuluDate = LocalDate.now()
    //      var autoCount: String = new String
    //      var blockids: String = new String
    //      var autoCountString: String = new String
    //      var resutlString:String = new String
    //      val sortedRDD = monitorRdd.map(data => {
    //        val dataArray = data._2.split("\\|")
    //        autoCount = dataArray(2)
    //        //println(autoCount)
    //        (data._1, autoCount)
    //      })
    //        .sortBy(_._2, false)
    //      val array = sortedRDD.take(num)
    //      array.foreach(data=>{
    //        blockids = blockids+data._1+"|"
    //        autoCountString = autoCountString+data._2+"|"
    //        //println(autoCountString)
    //      })
    //      blockids = blockids.dropRight(1)
    //      autoCountString = autoCountString.dropRight(1)
    //      val block = Array(zuluDate.toString+","+blockids+","+autoCountString)
    //      val saveTopNRDD = sc.parallelize(block)
    //        .map(data=>{
    //          val dataArray = data.split(",")
    //          topNBlock(dataArray(0),dataArray(1),dataArray(2))
    //        })
    //      val saveDF = saveTopNRDD.toDF()
    //        .createOrReplaceTempView("topNBlock")
    //      session.sql("select * from topNBlock")
    //        .write
    //        .mode(SaveMode.Append)
    //        .jdbc(JDBC_URL,"TopN_Count",properties)
    //    }
    //
    //
    //    /**
    //     * 统计分区速度排名
    //     * 封存于2020年4月6日
    //     * Done
    //     * 今日记事：Prime Minister Boris Johnson was sent to the hospital due to the COVID-19
    //     */
    //    //2018-04-26	0005	43356	深W61995	2018-04-26 02:13:57	174	32	05
    //
    //    val speedCount = sc.textFile("./monitor_flow_action")
    //      .map(data=>{
    //        val dataArray = data.split("\t")
    //        (dataArray(1),dataArray(5))
    //      })
    //      .groupByKey()
    //      //(0001,CompactBuffer(44, 26, 13, 31, 8, 177, 86, 140, 112, 77....)
    //      .map(data=>{
    //        var minSpeed: Int = 0
    //        var normalSpeed: Int = 0
    //        var medSpeed: Int = 0
    //        var highSpeed: Int = 0
    //
    //        val dataArray = data._2.toString().dropRight(1).drop(14).split(",")
    //        dataArray.foreach(data=>{
    //          val dataInt = data.trim.toInt
    //          avgSpeedAcc.add(dataInt)
    //          if (dataInt>0&&dataInt<60){
    //            minSpeed+=1
    //          }else if (dataInt>=60&&dataInt<90){
    //            normalSpeed+=1
    //          }else if (dataInt>=90&&dataInt<120){
    //            medSpeed+=1
    //          }else if (dataInt>=120){
    //            highSpeed+=1
    //          }
    //        })
    //        val avgSpeed:String = avgSpeedAcc.value.formatted("%.3f").toString
    //        avgSpeedAcc.reset()
    //        //println(highSpeed)
    //        (data._1,blockSpeedCount(minSpeed.toString,normalSpeed.toString,medSpeed.toString,highSpeed.toString,avgSpeed))
    //      })
    //      //.foreach(println)
    //      //(0001,blockSpeedCount(3151,1512,1553,1063,69.512))
    //    saveBlockSpeedRank(speedCount)
    //
    //    /**
    //     * 保存分区速度排名
    //     * Notice:试验，向数据库保存全量数据，并不采用take
    //     * 封存于2020年4月6日
    //     * Done
    //     */
    //    def saveBlockSpeedRank(speedCount:RDD[(String,blockSpeedCount)]):Unit = {
    //      //(0001,blockSpeedCount(3151,1512,1553,1063,69.512))
    //      val zuluTime = LocalDate.now()
    //      val speedRank = speedCount.map(data => {
    //        blockSpeedCount4Saving(zuluTime.toString, data._1, data._2.minSpeed, data._2.normalSpeed, data._2.medSpeed, data._2.highSpeed, data._2.avgSpeed)
    //      })
    //        .toDF()
    //        .createOrReplaceTempView("blockSpeedCount4Saving")
    //
    //      session.sql("select * from blockSpeedCount4Saving order by avg_Speed DESC")
    //        .write
    //        .mode(SaveMode.Append)
    //        .jdbc(JDBC_URL,"TopN_Speed",properties)
    //    }

//
//    /**
//     * 分析车辆轨迹
//     * 今日记事：Prime Minister was sent to the ICU due to the heavy symptom
//     */
//    //2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02
//
//    def getAutoRoutes(plate: String): Unit = {
//      var blockString: String = new String
//      var roadString: String = new String
//      var districtString: String = new String
//      var result: String = new String
//      val zuluTime = LocalDate.now()
//      val autoTrackResult = sc.textFile("./monitor_flow_action")
//        .filter(data => {
//          val dataArray = data.split("\t")
//          dataArray(3) == plate
//        })
//        .sortBy(data => {
//          val dataArray = data.split("\t")
//          dataArray(4)
//        })
//        .map(data => {
//          val dataArray = data.split("\t")
//          val track = autoTrack(zuluTime.toString, dataArray(3), dataArray(1), dataArray(6), dataArray(7))
//          (track.plate, track)
//        })
//        .groupByKey()
//        .map(data => {
//          val tracker = data._2
//          tracker.foreach(track => {
//            blockString = blockString + track.blockid + ">"
//            roadString = roadString + track.roadid + ">"
//            districtString = districtString + track.districtid + ">"
//          })
//          result = blockString.dropRight(1) + "|" + roadString.dropRight(1) + "|" + districtString.dropRight(1)
//          (data._1, result)
//        })
//      //.foreach(println)
//      saveAutoRoutes(autoTrackResult,zuluTime.toString,plate)
//
//    }
//
//    getAutoRoutes("辽G35232")
//
//
//    def saveAutoRoutes(autoTrackResult:RDD[(String,String)],zuluTime:String,plate:String):Unit = {
//      val autoTrackDF = autoTrackResult.map(data => {
//        val dataArray = data._2.split("\\|")
//        autoTrack(zuluTime,plate,dataArray(0).trim,dataArray(1).trim,dataArray(2).trim)
//      })
//        .toDF()
//        .createOrReplaceTempView("autoTrack")
//
//      session.sql("select * from autoTrack")
//        .write
//        .mode(SaveMode.Append)
//        .jdbc(JDBC_URL,"Auto_Track",properties)
//    }


//
//    /**
//     * Feature:外地车统计
//     *          统计外地车数量及首次出现位置
//     * 封存日期：2020年04月09日
//     * 今日记事：Wuhan -- the most affected city by COVID-19 is unlocked
//     */
//
//    def inboundAutoCount(localPlateStart:String):Unit ={
//      val inboundDF = sc.textFile("./monitor_flow_action")
//        .filter(data=>{
//          val dataArray = data.split("\t")
//          !dataArray(3).startsWith(localPlateStart)
//        })
//        .map(data=>{
//          //2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02
//          val dataArray = data.split("\t")
//          inboundAuto(dataArray(4),dataArray(3).trim,dataArray(1),dataArray(7))
//        })
//        .toDF()
//        .createOrReplaceTempView("inbound")
//      session.sql("select * from inbound order by inbound_date")
//        .write
//        .mode(SaveMode.Append)
//        .jdbc(JDBC_URL,"Inbound_Auto",properties)
//    }
//
//    inboundAutoCount("黑")
//
//
//    /**
//     * Feature:实时速度监控
//     * 封存日期：
//     * 今日记事：
//     *
//     */
//
//    val brokers ="localhost:9092"
//    val sourceTopic = "StreamAuto"
//    val targetTopic = "Target"
//    var group = "consumer-group"
//    val checkpoint = "/Users/skyline/Downloads/kafka_2.11-2.1.1/checkpoint"
//    ssc.checkpoint(checkpoint)
//
//    val kafkaParam = Map(
//      "bootstrap.servers" -> brokers,
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> group,
//      "auto.offset.reset" -> "latest"
//    )
//    val stream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Set(sourceTopic),kafkaParam))
//    //2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02
//    val AutoStreamRDD = stream
//      .map(data => {
//      val dataArray = data.toString.split("\t")
//      (dataArray(1), streamSpeed(dataArray(4).trim, dataArray(3).trim, dataArray(6).trim))
//    })
//
//    val unit = AutoStreamRDD.groupByKeyAndWindow(Seconds(10), Seconds(5))
//      .map(data => {
//        var speedCount: Int = 0
//        data._2.foreach(roadSpeed => {
//          speedCount += roadSpeed.speed.toInt
//        })
//        print(speedCount)
//        print(" ")
//        print(data._2.size)
//        (data._1, speedCount / data._2.size)
//      })
//      .foreachRDD(rdd => {
//        rdd.foreach(println)
//      })
//    //(0000,25)
//    ssc.start()
//    ssc.awaitTermination()




  }

}

