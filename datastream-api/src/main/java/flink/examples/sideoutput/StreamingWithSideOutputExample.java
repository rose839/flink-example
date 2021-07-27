package flink.examples.sideoutput;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
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
            .process(new Tokenizer());

        DataStream<String> rejectedWords = tokenized
            .getSideOutput(rejectedWordsTag)
            .map(new MapFunction<String, String>() {
                private static final long serialVersionUID = 1L;

                @Override
                public String map(String value) throws Exception {
                    return "rejected: " + value;
                }
            });

        DataStream<Tuple2<String, Integer>> counts = tokenized
            .keyBy(0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            // group by the tuple field "0" and sum up tuple field "1"
            .sum(1);

        System.out.println("Printing result to stdout. Use --output to specify output path.");
        counts.print();
        rejectedWords.print();

        try {
            // execute program
            env.execute("Streaming WordCount SideOutput");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     *
     * <p>This rejects words that are longer than 5 characters long.
     */
    public static final class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;


        @Override
        public void processElement(String value, Context context,
                Collector<Tuple2<String, Integer>> collector) throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 5) {
                    context.output(rejectedWordsTag, token);
                } else if (token.length() > 0) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
