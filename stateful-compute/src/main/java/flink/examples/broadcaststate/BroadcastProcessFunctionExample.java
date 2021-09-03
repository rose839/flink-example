package flink.examples.broadcaststate;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastProcessFunctionExample {
    public static void main(String[] args) {
        final MapStateDescriptor<Long, String> utterDescriptor = new MapStateDescriptor<>(
            "broadcast-state", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

        final Map<Long, String> expected = new HashMap<>();
        expected.put(0L, "test:100001");
        expected.put(1L, "test:100002");
        expected.put(2L, "test:100003");
        expected.put(3L, "test:100004");
        expected.put(4L, "test:100005");
        expected.put(5L, "test:100006");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        final DataStream<Long> srcOne = env.generateSequence(600006L, 600010L)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.
                    <Long>forBoundedOutOfOrderness(Duration.ofSeconds(5)).
                    withTimestampAssigner((event, Timestamp) -> event));
        

        final DataStream<String> srcTwo = env.fromCollection(expected.values())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.
                    <String>forBoundedOutOfOrderness(Duration.ofSeconds(5)).
                    withTimestampAssigner((event, Timestamp) -> Long.parseLong(event.split(":")[1])));
        
        final BroadcastStream<String> broadcast = srcTwo.broadcast(utterDescriptor);

        final DataStream<String> output = srcOne.connect(broadcast).process(
            new CustomBroadcastProcessFunction());

        output.print().setParallelism(1);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class CustomBroadcastProcessFunction extends
            BroadcastProcessFunction<Long, String, String> {

        private MapStateDescriptor<Long, String> descriptor;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            descriptor = new MapStateDescriptor<>(
                "broadcast-state", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        }

        @Override
        public void processElement(Long arg0, BroadcastProcessFunction<Long, String, String>.ReadOnlyContext arg1,
                Collector<String> arg2) throws Exception {
            System.out.println("value: " + arg0);
            for (Map.Entry<Long, String> e : arg1.getBroadcastState(descriptor).immutableEntries()) {
                System.out.println("state: " + e.getValue());
            }
        }

        @Override
        public void processBroadcastElement(String value, BroadcastProcessFunction<Long, String, String>.Context ctx,
                Collector<String> out) throws Exception {
            System.out.println("broadcast value: "+ value);
            long key = Long.parseLong(value.split(":")[1]);
            ctx.getBroadcastState(descriptor).put(key, value);
        }
    }
}
