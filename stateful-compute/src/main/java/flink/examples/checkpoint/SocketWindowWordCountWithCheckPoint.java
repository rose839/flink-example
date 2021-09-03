package flink.examples.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 配置checkpoint
*/
public class SocketWindowWordCountWithCheckPoint {
    public static void main(String[] args) {
        // 获取连接的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set. use default port 9099");
            port = 9099;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置checkpoint的周期，每隔1000ms启动一个检查点
        env.getCheckpointConfig().setCheckpointInterval(1000);

        // 设置模式为exactly-onece（这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置checkpoint的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        // 设置checkpoint的超时时间，检查点必须在指定时间内完成，否则会被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 同一时间只允许进行一个检查点，当设置了最小时间间隔时，该值必须为1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 表示一旦flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 设置statebackend
        env.setStateBackend(new FsStateBackend("file://" + System.getProperty("user.dir") + "/WorkSpace/checkpoints"));

        //连接socket获取输入的数据
        String hostname = "localhost";
        String delimiter = "\n";
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        DataStream<WordWithCount> windowCounts = text.flatMap((FlatMapFunction<String, WordWithCount>) (value, out) -> {
            String[] splits = value.split("\\s");
            for (String word : splits) {
                out.collect(new WordWithCount(word, 1L));
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))//指定时间窗口大小为2秒，指定时间间隔为1秒
                .sum("count");//在这里使用sum或者reduce都可以
                /*.reduce(new ReduceFunction<WordWithCount>() {
                                    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {

                                        return new WordWithCount(a.word,a.count+b.count);
                                    }
                                })*/
        //把数据打印到控制台并且设置并行度
        windowCounts.print().setParallelism(1);

        try {
            env.execute("Socket window count");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(String word,long count){
            this.word = word;
            this.count = count;
        }
        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
