package flink.examples.source.custom;

import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class CustomRichParallelSource extends RichParallelSourceFunction<Long> {
    private long count = 1L;

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while(isRunning){
            ctx.collect(count);
            count++;
            this.getRuntimeContext().getAccumulator("accumulator").add((double)count);

            Thread.sleep(1000);

            if (count > 10) {
                break;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    /**
     * This method will only be called once on starting(One instace one call).
    */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("Executing the open method: " + "ThreadName: " + Thread.currentThread().getName());
        this.getRuntimeContext().addAccumulator("accumulator",new AverageAccumulator());
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
