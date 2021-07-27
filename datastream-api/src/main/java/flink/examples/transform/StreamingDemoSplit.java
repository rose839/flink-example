package flink.examples.transform;

import java.util.ArrayList;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flink.examples.source.custom.CustomNoParallelSource;

public class StreamingDemoSplit {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text = env.addSource(new CustomNoParallelSource()).setParallelism(1);

        SplitStream<Long> splitStream = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> outPut = new ArrayList<>();
                if (value % 2 == 0) {
                    outPut.add("even");
                } else {
                    outPut.add("odd");
                }
                return outPut;
            }
        });
        
        DataStream<Long> evenStream = splitStream.select("even");
        DataStream<Long> oddStream = splitStream.select("odd");

        DataStream<Long> moreStream = splitStream.select("odd","even");
        moreStream.print();

        String jobName = StreamingDemoSplit.class.getSimpleName();
        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
