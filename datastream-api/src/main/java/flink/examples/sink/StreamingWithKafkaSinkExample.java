package com.geekbang.flink.sink;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

public class StreamingWithKafkaSinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9099, "\n");

        String brokerList = "127.0.0.1:9092";
        String topic = "test";

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", brokerList);

        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(
            topic, 
            new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()), 
            prop, 
            FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
        text.addSink(myProducer);

        env.execute("StreamingFromCollection");
    }
}
