package flink.examples.broadcaststate;

import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import flink.examples.broadcaststate.model.Pattern;

import java.io.IOException;

/**
 * The serialization schema for the {@link KafkaEvent} type. This class defines how to transform a
 * Kafka record's bytes to a {@link KafkaEvent}, and vice-versa.
 */
public class PatternEventSchema implements DeserializationSchema<Pattern>, SerializationSchema<Pattern> {
    private static final long serialVersionUID = 6154188370181669758L;

    @Override
    public byte[] serialize(Pattern event) {
        return event.toString().getBytes();
    }

    @Override
    public Pattern deserialize(byte[] message) throws IOException {
        return JSON.parseObject(message, Pattern.class);
    }

    @Override
    public boolean isEndOfStream(Pattern nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Pattern> getProducedType() {
        return TypeInformation.of(Pattern.class);
    }
}
