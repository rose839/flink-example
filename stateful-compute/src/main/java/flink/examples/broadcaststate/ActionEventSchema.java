package flink.examples.broadcaststate;

import flink.examples.broadcaststate.model.Action;

import java.io.IOException;

import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * The serialization schema for the {@link KafkaEvent} type. This class defines how to transform a
 * Kafka record's bytes to a {@link KafkaEvent}, and vice-versa.
 */
public class ActionEventSchema implements DeserializationSchema<Action>, SerializationSchema<Action> {
    private static final long serialVersionUID = 6154188370181669758L;

    @Override
    public byte[] serialize(Action element) {
        return element.toString().getBytes();
    };

    @Override
    public Action deserialize(byte[] message) throws IOException {
        JSON.parseObject(message, Action.class);
        return null;
    }

    @Override
    public boolean isEndOfStream(Action nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Action> getProducedType() {
        TypeInformation.of(Action.class);
        return null;
    }
}
