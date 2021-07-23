package flink.examples.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import flink.examples.source.custom.CustomParallelSource;

public class StreamingDemoWithMyPralalleSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Long>> text = env.addSource(new CustomParallelSource()).setParallelism(5);
        DataStream<Long> num = text.map(new MapFunction<Tuple2<String, Long>, Long>() {
            @Override
            public Long map(Tuple2<String, Long> input) throws Exception {
                System.out.println("recevie value：" + "ThreadName: " + input.f0 + " ,Value:" + input.f1);
                return input.f1;
            }
        });

        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(1);

        String jobName = StreamingDemoWithMyPralalleSource.class.getSimpleName();

        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
