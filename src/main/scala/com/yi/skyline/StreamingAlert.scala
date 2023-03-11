package com.yi.skyline

import java.util
import java.util.{Collections, Properties}

import com.yi.utils.SMSUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.Set

case class AutoAlerted(create_time: String, plate: String, blockid: String, time: String, roadid: String)


object StreamingAlert {


  /**
   *需求数据：
   *    plate：车牌号
   * 样例：
   *    黑A12345
   */
    
  val conf = new SparkConf()
    .set("spark.default.parallelism", "1")
  private val session: SparkSession = SparkSession.builder().appName("StreamingAlert")
    .master("local[2]")
    .config(conf)
    .getOrCreate()

  private val sc: SparkContext = session.sparkContext
  private val ssc = new StreamingContext(sc, Seconds(5))

  val secret_id = "AKIDFC8vYzPJvPp34tDN4MYTL5r7ACoDgSJz"
  val secret_key = "PeAf2DCVdXBi2HG0jeJk4AIE6vb5S3XB"
  val sms_app_id = "1400308849"
  val sms_sign = "Sin的专属空间"
  val sms_template_id = "587772"

  val brokers ="localhost:9092"
  val sourceTopic = "StreamAuto"
  val sourceTopicPlate = "streamingalert"
  val targetTopic = "Target"
  var group = "consumer-group"
  val kafkaParam = Map(
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> group,
    "auto.offset.reset" -> "latest"
  )


  def StreamingAlertDeploy(plate:String) : Unit ={

    var flag = 0
    val checkpoint = "/Users/skyline/Downloads/kafka_2.11-2.1.1/checkpointAlert"
    ssc.checkpoint(checkpoint)
    val stream = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Set(sourceTopic),kafkaParam))


    //2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02
    stream.map(_.value())
      .filter(data=>{
        val dataArray = data.split("\t")
        dataArray(3).equals(plate)
      })
      .map(data=>{
        val dataArray = data.split("\t")
        //val param:Array = [plate,dataArray(4).trim,dataArray(1).trim,dataArray(7).trim]
        SMSUtils.send(plate,dataArray(4).trim.drop(10).dropRight(3),"南岗中心","中山路")
        flag=1
        AutoAlerted(dataArray(0).trim,plate.trim,"南岗中心",dataArray(4).trim,dataArray(7).trim)
      })
      .foreachRDD(rdd=>{
        rdd.foreach(println)
      })
    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    var plate:String = new String
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
    kafkaConsumer.subscribe(Collections.singletonList("streamingalert"))
    while(true){
      val msgs: ConsumerRecords[String, String] = kafkaConsumer.poll(2000)
      val it = msgs.iterator()
      while(it.hasNext){
        val msg = it.next()
        plate = msg.value().dropRight(2).drop(10)
        if (plate!=""){
          //StreamingAlertDeploy("黑A3Z197")
          println(plate)
          StreamingAlertDeploy(plate)
        }
        plate=""
      }
    }


    //StreamingAlertDeploy("黑A3Z197")
  }
}
