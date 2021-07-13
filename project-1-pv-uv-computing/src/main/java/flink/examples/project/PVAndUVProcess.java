package flink.examples.project;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;

public class PVAndUVProcess {
    public static void main(String[] args) {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String kafkaTopic = parameterTool.get("kafka-topic", "default");
        String brokers = parameterTool.get("brokers", "127.0.0.1:9092");
        System.out.printf("Reading from kafka topic %s @ %s\n", kafkaTopic, brokers);

        Properties kafkaProps = new Properties();
        
    }
}
