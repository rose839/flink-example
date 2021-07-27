package flink.examples.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import flink.examples.source.custom.CustomNoParallelSource;

public class StreamingDemoConnect {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text1 = env.addSource(new CustomNoParallelSource()).setParallelism(1);
        DataStreamSource<Long> text2 = env.addSource(new CustomNoParallelSource()).setParallelism(1);
        SingleOutputStreamOperator<String> text2_str = text2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str_" + value;
            }
        });

        ConnectedStreams<Long, String> connectStream = text1.connect(text2_str);

        SingleOutputStreamOperator<Object> result = connectStream.map(new CoMapFunction<Long, String, Object>(){
            @Override
            public Object map1(Long value) throws Exception {
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        });

        result.print().setParallelism(1);

        String jobName = StreamingDemoConnect.class.getSimpleName();

        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
