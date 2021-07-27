package flink.examples.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import flink.examples.source.custom.CustomNoParallelSource;

public class StreamingDemoWithMyNoPralallelSourceBroadcast {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Long> text = env.addSource(new CustomNoParallelSource()).setParallelism(1);
    
        DataStream<Long> num = text.broadcast().map(new MapFunction<Long,Long>(){
            @Override
            public Long map(Long value) throws Exception {
                long id = Thread.currentThread().getId();
                System.out.println("Thread id: " + id + ", recv value: " + value);
                return value;
            }
        });

        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        String jobName = StreamingDemoWithMyNoPralallelSourceBroadcast.class.getSimpleName();
        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
