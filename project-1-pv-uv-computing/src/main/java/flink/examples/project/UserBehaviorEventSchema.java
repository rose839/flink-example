package flink.examples.project;

import java.io.IOException;

import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class UserBehaviorEventSchema implements SerializationSchema<UserBehaviorEvent>, DeserializationSchema<UserBehaviorEvent> {
    @Override
    public byte[] serialize(UserBehaviorEvent event) {
        return event.toString().getBytes();
    }

    @Override
    public UserBehaviorEvent deserialize(byte[] message) throws IOException {
        return JSON.parseObject(message, UserBehaviorEvent.class);
    }

    @Override
    public boolean isEndOfStream(UserBehaviorEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserBehaviorEvent> getProducedType() {
        return TypeInformation.of(UserBehaviorEvent.class);
    }
}
