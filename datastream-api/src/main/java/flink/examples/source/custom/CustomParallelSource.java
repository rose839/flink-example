package flink.examples.source.custom;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class CustomParallelSource implements ParallelSourceFunction<Tuple2<String, Long>> {
    private long count = 1L;

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        String threadName = Thread.currentThread().getName();
        while(isRunning) {
            ctx.collect(new Tuple2<>(threadName, count));
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    } 
}
