package com.yi.skyline

import java.io.{File, PrintWriter}
import java.time.LocalDate
import java.util.{Collections, Properties}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import com.yi.accumulator.{MonitorAccumulator, MonitorCountAccumulator, SpeedAccumulator}
import com.yi.skyline.MonitorFlowAnalyze.result
import com.yi.skyline.StreamingAlert.StreamingAlertDeploy
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class autoTrack(create_date: String, plate: String, blockid: String, roadid: String, districtid: String)

object AutoTrackAnalyze {

  /**
   * 提供数据：
   * blockid：分区id
   * districtid：行政区名称
   * roadid：道路名称
   * plate：车牌号
   * create_date：创建日期
   * 显示说明：
   * 车牌号：道路名称>道路名称>...>道路名称
   */

  /**
   * 需求数据：
   * 样例：黑A12345
   * 存储位置：当前目录/input_data.log
   *
   */


  val conf = new SparkConf()
    .set("spark.default.parallelism", "1")
  val session = SparkSession.builder().appName("AutoTrackAnalyze")
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
  val districtFrame: DataFrame = session.read
    .jdbc(JDBC_URL, "district_info", properties)
  districtFrame.show()
  districtFrame.createOrReplaceTempView("district")
  session.read
    .jdbc(JDBC_URL, "road_info", properties)
    .createOrReplaceTempView("roadInfo")

  def main(args: Array[String]): Unit = {
    var plate: String = new String
    val prop = new Properties
    prop.put("bootstrap.servers", "localhost:9092")
    // 指定消费者组
    prop.put("group.id", "group01")
    // 指定消费位置: earliest/latest/none
    prop.put("auto.offset.reset", "latest")
    // 指定消费的key的反序列化方式
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 指定消费的value的反序列化方式
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 得到Consumer实例
    val kafkaConsumer = new KafkaConsumer[String, String](prop)
    kafkaConsumer.subscribe(Collections.singletonList("autotrack"))
    while (true) {
      val msgs: ConsumerRecords[String, String] = kafkaConsumer.poll(2000)
      val it = msgs.iterator()
      while (it.hasNext) {
        val msg = it.next()
        plate = msg.value().dropRight(2).drop(10)
        if (plate != "") {
          //StreamingAlertDeploy("黑A3Z197")
          println(plate)
          //getAutoRoutes("辽G35232")
          getAutoRoutes(plate)
        }
        plate = ""
      }

    }


    /**
     * 分析车辆轨迹
     * 今日记事：Prime Minister was sent to the ICU due to the heavy symptom
     */
    //2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02

    def getAutoRoutes(plate: String): Unit = {
      var blockString: String = new String
      var roadString: String = new String
      var districtString: String = new String
      var result: String = new String
      val zuluTime = LocalDate.now()
      val autoTrackResult = sc.textFile("./monitor_flow_action")
        .filter(data => {
          val dataArray = data.split("\t")
          dataArray(3) == plate
        })
        .sortBy(data => {
          val dataArray = data.split("\t")
          dataArray(4)
        })
        .map(data => {
          val dataArray = data.split("\t")
          val track = autoTrack(zuluTime.toString, dataArray(3), dataArray(1), dataArray(6), dataArray(7))
          (track.plate, track)
        })
        .groupByKey()
        .map(data => {
          val tracker = data._2
          tracker.foreach(track => {
            blockString = blockString + track.blockid + ">"
            roadString = roadString + track.roadid + ">"
            districtString = districtString + track.districtid + ">"
          })
          result = blockString.dropRight(1) + "|" + roadString.dropRight(1) + "|" + districtString.dropRight(1)
          (data._1, result)
        })
      //.foreach(println)
      //saveAutoRoutes(autoTrackResult, zuluTime.toString, plate)
      saveAutoRoutesToFile(autoTrackResult, zuluTime.toString, plate)

    }

    //      getAutoRoutes("辽G35232")


    def saveAutoRoutes(autoTrackResult: RDD[(String, String)], zuluTime: String, plate: String): Unit = {
      val autoTrackDF = autoTrackResult.map(data => {
        val dataArray = data._2.split("\\|")
        autoTrack(zuluTime, plate, dataArray(0).trim, dataArray(1).trim, dataArray(2).trim)
      })
        .toDF()
        .createOrReplaceTempView("autoTrack")

      session.sql("select * from autoTrack")
        .write
        .mode(SaveMode.Append)
        .jdbc(JDBC_URL, "Auto_Track", properties)
    }

    def saveAutoRoutesToFile(autoTrackResult: RDD[(String, String)], zuluTime: String, plate: String): Unit = {
      val autoTrackDF = autoTrackResult.map(data => {
        val dataArray = data._2.split("\\|")
        val result = autoTrack(zuluTime, plate, dataArray(0).trim, dataArray(1).trim, dataArray(2).trim)
        val conf = new SerializeConfig(true)
        JSON.toJSONString(result, conf)
      })
        //{"blockid":"0000>0005>0007>0008>0007>0002>0006>0002>0008>0001>0005>0000>0002>0006>0000>0001>0001>0007>0001>0000>0001>0006>0000>0007>0004>0006>0003>0002>0006>0007>0001>0000>0008>0006>0006>0004>0003>0004>0007>0006>0004>0002>0004>0000",
        // "create_date":"2020-04-19",
        // "districtid":"07>07>03>05>07>06>07>01>01>06>08>03>02>08>03>01>06>04>06>05>03>07>03>06>08>06>01>01>03>05>08>05>06>06>05>08>01>04>04>04>05>06>07>03",
        // "plate":"辽G35232",
        // "roadid":"23>2>22>33>37>39>14>11>28>28>50>42>49>33>9>12>46>3>25>1>49>18>7>26>17>32>24>34>42>15>27>43>6>22>42>16>49>6>22>49>5>44>1>21"}
        .foreach(data => {
          var district_display: String = new String
          var road_display: String = new String
          val jo = JSON.parseObject(data)
          jo.getString("districtid")
            .split(">")
            .foreach(data => {
              //println(data)
              val DIDframe = session.sql("select district_name from district where district_id =" + data)
              val value = DIDframe.collectAsList()
              district_display = district_display + value.get(0) + ">"
            })
          jo.getString("roadid")
            .split(">")
            .foreach(data => {
              val frame = session.sql("select road_name from roadInfo where road_id = " + data)
              val value = frame.collectAsList()
              road_display = road_display + value.get(0) + ">"
            })
          jo.put("districtid", district_display.dropRight(1))
          jo.put("roadid", road_display.dropRight(1))
          val conf = new SerializeConfig(true)
          val str1 = JSON.toJSONString(jo, conf)
          val writer = new PrintWriter(new File("/Users/skyline/Downloads/bigdata/autotrack.log"))
          writer.println(str1)
          writer.close()
        })
      //{"blockid":"0000>0005>0007>0008>0007>0002>0006>0002>0008>0001>0005>0000>0002>0006>0000>0001>0001>0007>0001>0000>0001>0006>0000>0007>0004>0006>0003>0002>0006>0007>0001>0000>0008>0006>0006>0004>0003>0004>0007>0006>0004>0002>0004>0000",
      // "districtid":"[呼兰区]>[呼兰区]>[道外区]>[香坊区]>[呼兰区]>[阿城区]>[呼兰区]>[南岗区]>[南岗区]>[阿城区]>[ZYHB沈阳情报区哈尔滨太平机场]>[道外区]>[道里区]>[ZYHB沈阳情报区哈尔滨太平机场]>[道外区]>[南岗区]>[阿城区]>[松北区]>[阿城区]>[香坊区]>[道外区]>[呼兰区]>[道外区]>[阿城区]>[ZYHB沈阳情报区哈尔滨太平机场]>[阿城区]>[南岗区]>[南岗区]>[道外区]>[香坊区]>[ZYHB沈阳情报区哈尔滨太平机场]>[香坊区]>[阿城区]>[阿城区]>[香坊区]>[ZYHB沈阳情报区哈尔滨太平机场]>[南岗区]>[松北区]>[松北区]>[松北区]>[香坊区]>[阿城区]>[呼兰区]>[道外区]",
      // "roadid":"[经纬街]>[发展大道]>[通达街]>[中源大道]>[和平路]>[三大动力路]>[中央大街]>[哈尔滨大街]>[南通大街]>[南通大街]>[哈尔滨绕城高速]>[通乡街]>[ZYHB机场高速]>[中源大道]>[测绘路]>[哈西大街]>[上京大道]>[哈双路]>[南十四道街]>[城乡路]>[ZYHB机场高速]>[安隆街]>[北航路]>[太古街]>[工程路]>[松北大道]>[北新街]>[世茂大道]>[通乡街]>[友谊路]>[滨江街]>[公滨路]>[学府路]>[通达街]>[通乡街]>[爱建路]>[ZYHB机场高速]>[学府路]>[通达街]>[ZYHB机场高速]>[保健路]>[延川大街]>[城乡路]>[新阳路]",
      // "plate":"辽G35232",
      // "create_date":"2020-04-20"}
      //.foreach(println)
      //.saveAsTextFile("/Users/skyline/Desktop/TestData/AutoTrack")
    }

  }
}
