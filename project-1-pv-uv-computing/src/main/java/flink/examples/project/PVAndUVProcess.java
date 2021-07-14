package flink.examples.project;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

public class PVAndUVProcess {
    public static void main(String[] args) {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String kafkaTopic = parameterTool.get("kafka-topic", "default");
        String brokers = parameterTool.get("brokers", "127.0.0.1:9092");
        System.out.printf("Reading from kafka topic %s @ %s\n", kafkaTopic, brokers);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", brokers);
        FlinkKafkaConsumer010<UserBehaviorEvent> kafka = new FlinkKafkaConsumer010<>(kafkaTopic, new UserBehaviorEventSchema(), kafkaProps);
        kafka.setStartFromLatest();
        kafka.setCommitOffsetsOnCheckpoints(false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        kafka.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                .withTimestampAssigner((ctx) -> new TimeStampExtractor()));

        DataStreamSource<UserBehaviorEvent> dataStreamByEventTime = env.addSource(kafka);
        
        DataStream<Tuple4<Long, Long, Long, Integer>> 
    }

    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<UserBehaviorEvent>, Serializable {
        private long currentWatermark = Long.MIN_VALUE;

        @Override
        public void onEvent(UserBehaviorEvent event, long eventTimestamp, WatermarkOutput output) {
            currentWatermark = eventTimestamp;
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            long effectiveWatermark =
                    currentWatermark == Long.MIN_VALUE ? Long.MIN_VALUE : currentWatermark - 1;
            output.emitWatermark(new Watermark(effectiveWatermark)); 
        }
    }

    private static class TimeStampExtractor implements TimestampAssigner<UserBehaviorEvent> {
        @Override
        public long extractTimestamp(UserBehaviorEvent element, long recordTimestamp) {
            return element.getTs();
        }
    }
}
