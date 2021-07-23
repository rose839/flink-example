package flink.examples.watermark;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class StreamingWindowWatermark {
    public static void main(String[] args) {
        int port = 9099;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * Default is number of cpu cores if not set.
        */
        env.setParallelism(1);

        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");

        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });

        DataStream<Tuple2<String, Long>> watermarkStream = inputMap.assignTimestampsAndWatermarks(
            WatermarkStrategy.
                <Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10)).
                withTimestampAssigner((event, Timestamp) -> event.f1)
        );

        DataStream<String> window = watermarkStream.keyBy(event -> event.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(3)))
            .apply(new WindowFunction<Tuple2<String,Long>,String,String,TimeWindow>(){
                @Override
                public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) {
                    List<Long> arrayList = new ArrayList<Long>();
                    Iterator<Tuple2<String, Long>> it = input.iterator();
                    while (it.hasNext()) {
                        Tuple2<String, Long> next = it.next();
                        System.out.println(next.f0+":"+next.f1);
                        arrayList.add(next.f1);
                    }

                    Collections.sort(arrayList);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    String result = key + "," + arrayList.size() + "," + sdf.format(arrayList.get(0)) + "," + sdf.format(arrayList.get(arrayList.size() - 1))
                                + "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
                    out.collect(result);
                }
            } );

        window.print();
        
        try {
            env.execute("eventtime-watermark");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
