package com.yi.skyline

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.yi.accumulator.{CurrentAutoAccumulator, MonitorAccumulator, MonitorCountAccumulator, SpeedAccumulator}
import com.yi.skyline.StreamingAlert.ssc
import com.yi.utils.{SCPUtils, SFTPUtils}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import redis.clients.jedis.Jedis

import scala.collection.mutable.Set

case class streamSpeed(create_date: String, plate: String, speed: String)
case class streamAutoToSave(create_date:String,currentAutoCount:String,currentInboundAuto:String,speedRank:String)
case class streamAutoRayData(create_date:String,currenAutoCount:String,currentInboundAuto:String,blockRank:String,speedRank:String)
case class streamAutoProvinceRayData(provinceList:String,provinceCount:String)

object StreamingSpeedCount {
  var accValue:Int = 0
  var accValueA:Int = 0
  val conf = new SparkConf()
    .set("spark.default.parallelism", "1")
  val session = SparkSession.builder().appName("Monitor")
    .enableHiveSupport()
    .master("local[2]")
    .config(conf)
    .getOrCreate()
  import session.implicits._
  val sc = session.sparkContext
  val ssc = new StreamingContext(sc, Seconds(10))
  val currentAcc = new CurrentAutoAccumulator
  sc.register(currentAcc,"currentAcc")
  var tempResult:String = new String
  var result:String = new String
  var blockResult:String = new String
  var speedResult:String = new String
  var provinceCountResult:String = new String

  val provinceMap = Map[String,String](
    "黑"->"黑龙江",
    "京"->"北京",
    "沪"->"上海",
    "琼"->"海南",
    "辽"->"辽宁",
    "吉"->"吉林",
    "闽"->"福建"
  )
  val provinceList = Array[String]("黑龙江","北京","上海","海南","辽宁","吉林","福建")
  var provinceArray = Array.ofDim[String](7,2)
  var provinceResult:String = new String







  def main(args: Array[String]): Unit = {


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

    for (elem <- provinceList) {
      provinceResult = provinceResult + elem +"|"
    }


    /**
     * Feature:实时速度监控
     * 封存日期：
     * 今日记事：
     *
     */

    /**
     * 提供数据：
     *      create_date：创建时间
     *      currentAutoCount：实施车辆数量
     *      currentInboundAuto：实时外地车辆数量
     *      speedRank：实时速度
     */

    val brokers = "localhost:9092"
    val sourceTopic = "StreamAuto"
    val sourceTopicPlate = "streamingalert"
    val targetTopic = "Target"
    var group = "abc"
    val checkpoint = "/Users/skyline/Downloads/kafka_2.11-2.1.1/checkpoint"
    ssc.checkpoint(checkpoint)



    val kafkaParam = Map(
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest"
    )
    val stream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Set(sourceTopic), kafkaParam))
    //2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02
    val AutoStreamRDD = stream
      .map(data => {
        val dataArray = data.toString.split("\t")
        (dataArray(1), streamSpeed(dataArray(4).trim, dataArray(3).trim, dataArray(6).trim))
      })

    stream.foreachRDD(rdd=>{
      rdd.foreach(data=>{
        currentAcc.add(data.toString)
//        val dataArray = data.toString.split("\t")
//        if(!dataArray(3).startsWith("黑A")){
//          accValueA+=1
//        }
//        accValue+=1
//        print(accValue)
//        print(" ")
//        println(accValueA)
      })
    })

    val value = AutoStreamRDD.groupByKeyAndWindow(Seconds(20), Seconds(10))
      .map(data => {
        var speedCount: Int = 0
        data._2.foreach(roadSpeed => {
          speedCount += roadSpeed.speed.toInt
        })
        //print(speedCount)
        //print(" ")
        //print(data._2.size)
        (data._1, speedCount / data._2.size)
      })

      //      .foreachRDD(rdd => {
      //        rdd.foreach(data=>{
      //          tempResult= tempResult+">"+data
      //        })
      //        streamAutoToSave(LocalDate.now().toString,currentAcc.value._1,currentAcc.value._2,tempResult)
      //      })
      value.foreachRDD(rdd => {
        rdd.foreach(data=>{
          tempResult = tempResult + ">" + data.toString()
        })
        val save = streamAutoToSave(LocalDate.now().toString, currentAcc.value._1, currentAcc.value._2, tempResult.drop(1))
        //val save = streamAutoToSave(LocalDate.now().toString, accValue.toString, accValueA.toString, data)
        val conf = new SerializeConfig(true)
        result = JSON.toJSONString(save, conf)
        //val writer = new PrintWriter(new File("/Users/skyline/Desktop/TestData/StreamingAutoCount/AutoCount.log"))
        val writer = new PrintWriter(new File("/Users/skyline/Downloads/bigdata/AutoCount.log"))
        writer.println(result)
        writer.close()
        result = ""
        tempResult = ""

      })
    speedRankRayData(value)
    ProvinceCount(AutoStreamRDD)

    val plateStream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Set(sourceTopicPlate),kafkaParam))



//    for(i <- 1 to 100)
//      writer.println(i)
//    writer.close()
    //(0000,25)
    ssc.start()
    ssc.awaitTermination()

  }


  /**
   * 速度排名 to RayData
   * @param value
   */
  def speedRankRayData(value:DStream[(String,Int)]) : Unit ={
    value.foreachRDD(rdd =>{
      rdd.foreach(data=>{
        blockResult = blockResult+data._1+"|"
        speedResult = speedResult+data._2+"|"
      })
      val save = streamAutoRayData(LocalDate.now().toString, currentAcc.value._1, currentAcc.value._2,blockResult.dropRight(1),speedResult.dropRight(1) )
      //val save = streamAutoToSave(LocalDate.now().toString, accValue.toString, accValueA.toString, data)
      val conf = new SerializeConfig(true)
      val resultRay  = JSON.toJSONString(save, conf)
      //val writer = new PrintWriter(new File("/Users/skyline/Desktop/TestData/StreamingAutoCount/AutoCount.log"))
      val writer = new PrintWriter(new File("/Users/skyline/Downloads/bigdata/AutoCountRay.log"))
      writer.println(resultRay)
      writer.close()
      val remote = new SCPUtils("/Users/skyline/Downloads/bigdata/AutoCountRay.log")
      remote.scpTo()
      blockResult = ""
      speedResult = ""
    })

  }


  /**
   * 省份输入排名
   * @param AutoStreamRDD
   */
  def ProvinceCount(AutoStreamRDD:DStream[(String,streamSpeed)]):Unit ={
    //private static final String[] locations = new String[]{"黑","黑","黑","京","沪","琼","黑","辽","吉","闽"};
    var i:Int = 0
    for (elem <- provinceList) {
      provinceArray(i)(0) = elem
      provinceArray(i)(1) = "0"
      i+=1
    }
    AutoStreamRDD.foreachRDD(rdd=>{
      rdd.foreach(data=>{
        val index = provinceMap.get(data._2.plate.charAt(0).toString).toString
        val province = provinceList.indexOf(index.dropRight(1).drop(5))
        println("INDEX:"+province)
        provinceArray(province)(1) = (provinceArray(province)(1).toInt+1).toString
      })
      for (i<-0 to 6){
        provinceCountResult = provinceCountResult + provinceArray(i)(1)+"|"
        println("PROVINCE:"+provinceArray(i)(1))
      }
      val resultString = streamAutoProvinceRayData(provinceResult.dropRight(1),provinceCountResult.dropRight(1))
      val conf = new SerializeConfig(true)
      val resultRay  = JSON.toJSONString(resultString, conf)
      //val writer = new PrintWriter(new File("/Users/skyline/Desktop/TestData/StreamingAutoCount/AutoCount.log"))
      val writer = new PrintWriter(new File("/Users/skyline/Downloads/bigdata/ProvinceCountRay.log"))
      writer.println(resultRay)
      writer.close()
//      val ftp = new SFTPUtils("192.168.1.43", 22, "lijiaqi", "199888")
//      ftp.upload("C://", "/Users/skyline/Downloads/bigdata/ProvinceCountRay.log", false)
//      ftp.closeChannel()
      val remote = new SCPUtils("/Users/skyline/Downloads/bigdata/ProvinceCountRay.log")
      remote.scpTo()
      provinceCountResult = ""
    })
  }



}
