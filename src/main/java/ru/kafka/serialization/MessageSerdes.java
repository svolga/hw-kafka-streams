package ru.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import ru.kafka.domain.Message;

import java.io.IOException;
import java.util.Map;

public class MessageSerdes implements Serde<Message> {

    @Override
    public Serializer<Message> serializer() {
        return new MessageSerializer();
    }

    @Override
    public Deserializer<Message> deserializer() {
        return new MessageDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    public static class MessageSerializer implements Serializer<Message> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public byte[] serialize(String topic, Message data) {
            try {
                if (data == null) {
                    return null;
                }
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing Message", e);
            }
        }
    }

    public static class MessageDeserializer implements Deserializer<Message> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Message deserialize(String topic, byte[] data) {
            try {
                if (data == null || data.length == 0) {
                    return null;
                }
                return objectMapper.readValue(data, Message.class);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing Message", e);
            }
        }
    }
}