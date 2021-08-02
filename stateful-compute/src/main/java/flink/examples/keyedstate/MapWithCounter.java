package flink.examples.keyedstate;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapWithCounter {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<String, String>> data = new ArrayList<>();
        data.add(new Tuple2<String, String>("hello", "world"));
        data.add(new Tuple2<String, String>("hello", "world"));
        data.add(new Tuple2<String, String>("hello", "world"));

        DataStreamSource<Tuple2<String, String>> collectionData = env.fromCollection(data);
        collectionData.
            keyBy(new KeySelector<Tuple2<String, String>, String>() {
                @Override
                public String getKey(Tuple2<String, String> value) throws Exception {
                    return value.f1;
                }
            }).
            map(new MapWithCounterFunction()).print();

        try {
            env.execute("job");
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}

class MapWithCounterFunction extends RichMapFunction<Tuple2<String, String>, Long> {
    private ValueState<Long> totalLengthByKey;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Long> stateDescriptor = 
            new ValueStateDescriptor<Long>("Sum of length", LongSerializer.INSTANCE);
        totalLengthByKey = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public Long map(Tuple2<String, String> value) throws Exception {
        Long length = totalLengthByKey.value();
        if (length == null) {
            length = 0L;
        }

        long newTotalLength = length + value.f1.length();
        totalLengthByKey.update(newTotalLength);
        return newTotalLength;
    }
}
