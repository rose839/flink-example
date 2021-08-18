package flink.examples.operatorstate;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapWithCheckpoiontedFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Long> data = new ArrayList<>();
        data.add(1L);
        data.add(2L);
        data.add(3L);
        data.add(3L);
        data.add(3L);

        DataStreamSource<Long> collectionData = env.fromCollection(data);
        collectionData.
            keyBy(new KeySelector<Long, Long>() {
                @Override
                public Long getKey(Long value) throws Exception {
                    return value;
                }
            }).
            map(new CustomMapFunction<Long>()).print();

        try {
            env.execute("job");
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    static class CustomMapFunction<T> implements MapFunction<T, T>, CheckpointedFunction {
        private ReducingState<Long> countPerKey;
        private ListState<Long> countPerPartition;
        private long localCount;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            countPerKey = context.getKeyedStateStore().getReducingState(new ReducingStateDescriptor<>("perKeyCount", new AddFunction(), Long.class));
            countPerPartition = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("perPartitionCount", Long.class));
            for (Long l : countPerPartition.get()) {
                localCount += l;
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            countPerPartition.clear();
            countPerPartition.add(localCount);
        }

        @Override
        public T map(T value) throws Exception {
            countPerKey.add(1L);
            localCount++;
            return value;
        }
    }

    static class AddFunction implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long aLong, Long t1) throws Exception {
            return aLong + t1;
        }
    }
}
