package flink.examples.sideoutput;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

public class StreamingWithSideOutputExample {
    private static final OutputTag<String> rejectedWordsTag = new OutputTag<>("rejected"){};

    public static void main(String[] args) {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<String> text = env.readTextFile(params.get("input"));

        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = text
            .keyBy(new KeySelector<String,Integer>(){
                private static final long serialVersionUID = 1L;

                @Override
                public Integer getKey(String value) throws Exception {
                    return 0;
                }
            })
            .process();
    }
}
