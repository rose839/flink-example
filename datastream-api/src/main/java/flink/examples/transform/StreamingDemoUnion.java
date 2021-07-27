package flink.examples.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import flink.examples.source.custom.CustomNoParallelSource;

public class StreamingDemoUnion {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text1 = env.addSource(new CustomNoParallelSource()).setParallelism(1);//注意：针对此source，并行度只能设置为1

        DataStreamSource<Long> text2 = env.addSource(new CustomNoParallelSource()).setParallelism(1);

        DataStream<Long> text = text1.union(text2);

        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("Recv value：" + value);
                return value;
            }
        });

        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        String jobName = StreamingDemoUnion.class.getSimpleName();
        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
