package flink.examples.project;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaMessageProducer {

    static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
    static SimpleDateFormat sdf2 = new SimpleDateFormat("HH:mm:ss");

    public static void main(String[] args) {
        Map<String, Object> kafkaParam = new HashMap<>(3);
        String topic = args.length == 0 ? Constants.TOPIC : args[0];
        System.out.println("produce-topic" + topic);
        kafkaParam.put("bootstrap.servers", Constants.KAFKA_BOOTSTRAP_SERVERS_KEY);
        kafkaParam.put("key.serializer", StringSerializer.class.getName());
        kafkaParam.put("value.serializer", StringSerializer.class.getName());
    
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaParam);
        int index = 0;
        while (index < 100000) {
            String msg = genMessage();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
            kafkaProducer.send(record);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            index++;
        }

        kafkaProducer.close();
    }

    private static String genMessage() {
        Date date = new Date();
        StringBuffer ts = new StringBuffer();
        ts.append(sdf1.format(date))
            .append("T")
            .append(sdf2.format(date))
            .append("Z");
        String[] action = {"click", "bug", "login", "logout"};
        String[] category = {"c1", "c2", "c3", "c4"};
    }
}
