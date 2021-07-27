package flink.examples.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import flink.examples.source.custom.CustomNoParallelSource;

public class StreamingDemoFilter {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new CustomNoParallelSource()).setParallelism(1);

        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value){
                System.out.println("Recv value：" + value);
                return value;
            }
        });

        DataStream<Long> filterData = num.filter(new FilterFunction<Long>(){
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });

        DataStream<Long> resultData = filterData.map(new MapFunction<Long, Long>(){
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("Filtered value: " + value);
                return value;
            }
        });

        DataStream<Long> sum = resultData.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        String jobName = StreamingDemoFilter.class.getSimpleName();
        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
