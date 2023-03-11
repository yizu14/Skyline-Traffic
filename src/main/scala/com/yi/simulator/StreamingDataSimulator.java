package com.yi.simulator;

import com.yi.utils.DateUtils;
import com.yi.utils.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;


public class StreamingDataSimulator {

    private static final Random random = new Random();
    private static final String[] locations = new String[]{"黑","黑","黑","京","沪","琼","黑","辽","吉","闽"};


    public static void main(String[] args) {
        //2018-04-26	0005	26780	深W61995	2018-04-26 02:44:08	154	47	02
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        while(true){
            String date = DateUtils.getTodayDate();
            String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24)+"");
            baseActionTime = date + " " + StringUtils.fulfuill((Integer.parseInt(baseActionTime.split(" ")[1])+1)+"");
            String actionTime = baseActionTime + ":" + StringUtils.fulfuill(random.nextInt(60)+"") + ":" + StringUtils.fulfuill(random.nextInt(60)+"");
            String monitorId = StringUtils.fulfuill(4, random.nextInt(9)+"");
            String car = locations[random.nextInt(10)] + (char)(65+random.nextInt(26))+StringUtils.fulfuill(5,random.nextInt(99999)+"");
            String speed = random.nextInt(140)+"";
            String roadId = random.nextInt(50)+1+"";
            String cameraId = StringUtils.fulfuill(5, random.nextInt(9999)+"");
            String areaId = StringUtils.fulfuill(2,random.nextInt(8)+"");
            String result = date+"\t"+monitorId+"\t"+cameraId+"\t"+car + "\t" + actionTime + "\t" + speed + "\t" + roadId + "\t" + areaId;
            ProducerRecord<String, String> streamAuto = new ProducerRecord<>("StreamAuto", result);
            //String testCase = date+"\t"+monitorId+"\t"+cameraId+"\t"+ "黑A3Z197" + "\t" + actionTime + "\t" + speed + "\t" + roadId + "\t" + areaId;
            //ProducerRecord<String, String> streamAutoTestCase = new ProducerRecord<>("StreamAuto", testCase);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(streamAuto);
            //producer.send(streamAutoTestCase);
        }
    }
}
