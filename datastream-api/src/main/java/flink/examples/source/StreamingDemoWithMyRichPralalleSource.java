package flink.examples.source;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import flink.examples.source.custom.CustomRichParallelSource;

public class StreamingDemoWithMyRichPralalleSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new CustomRichParallelSource()).setParallelism(2);

        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("receive value：" + value);
                return value;
            }
        });

        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(1);

        String jobName = StreamingDemoWithMyRichPralalleSource.class.getSimpleName();

        try {
            JobExecutionResult result = env.execute(jobName);

            /** 
             * Muti accumulator instance will be merged.
             * */
            System.out.println("average: " + (double)result.getAccumulatorResult("accumulator"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
