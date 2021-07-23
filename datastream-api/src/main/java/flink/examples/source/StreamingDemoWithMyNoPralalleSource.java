package flink.examples.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import flink.examples.source.custom.CustomNoParallelSource;

public class StreamingDemoWithMyNoPralalleSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * Parallelism must be 1 on no parallel source.
        */
        DataStreamSource<Long> text = env.addSource(new CustomNoParallelSource()).setParallelism(1);

        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("Receive value：" + value);
                return value;
            } 
        });

        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        DataStream<Long> max = num.timeWindowAll(Time.seconds(10)).max(0);

        sum.print().setParallelism(1);
        max.print().setParallelism(1);
        String jobName = StreamingDemoWithMyNoPralalleSource.class.getSimpleName();

        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
