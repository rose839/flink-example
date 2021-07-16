package flink.examples.project;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

public class PVAndUVProcess {
    public static void main(String[] args) {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String kafkaTopic = parameterTool.get("kafka-topic", Constants.TOPIC);
        String brokers = parameterTool.get("brokers", Constants.KAFKA_BOOTSTRAP_SERVER);
        System.out.printf("Reading from kafka topic %s @ %s\n", kafkaTopic, brokers);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", brokers);
        FlinkKafkaConsumer010<UserBehaviorEvent> kafka = new FlinkKafkaConsumer010<>(kafkaTopic, new UserBehaviorEventSchema(), kafkaProps);
        kafka.setStartFromEarliest();
        kafka.setCommitOffsetsOnCheckpoints(false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        kafka.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                .withTimestampAssigner((ctx) -> new TimeStampExtractor()));

        DataStreamSource<UserBehaviorEvent> dataStreamByEventTime = env.addSource(kafka);
        
        DataStream<Tuple4<Long, Long, Long, Integer>> uvCounter = dataStreamByEventTime.
            windowAll(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1))).
            process(new ProcessAllWindowFunction<UserBehaviorEvent, Tuple4<Long, Long, Long, Integer>, TimeWindow>() {
                @Override
                public void process(Context context, Iterable<UserBehaviorEvent> elements, Collector<Tuple4<Long, Long, Long, Integer>> out) {
                    System.out.println("windows process");
                    Long pv = 0L;
                    Set<Integer> userIds = new HashSet<>();
                    Iterator<UserBehaviorEvent> iterator = elements.iterator();
                    while (iterator.hasNext()) {
                        UserBehaviorEvent userBehavior = iterator.next();
                        pv++;
                        userIds.add(userBehavior.getUserID());
                        System.out.println(userBehavior.getTs());
                    }
                    TimeWindow window = context.window();
                    out.collect(new Tuple4<>(window.getStart(), window.getEnd(), pv, userIds.size()));
                }
            });

        uvCounter.print().setParallelism(1);

        try {
            env.execute(parameterTool.get("appName", "PVAndUVExample"));
        } catch (Exception e) {
            e.printStackTrace();
        }
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
