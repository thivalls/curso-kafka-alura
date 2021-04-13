package com.br.zup.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.regex.Pattern;

public class GenericLogService {
    public static void main(String[] args) {
        var genericLogService = new GenericLogService();
        var kafkaService = new KafkaService(
                GenericLogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                genericLogService::parse,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
        );
        kafkaService.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("---------------------------------------------");
        System.out.println("Generic logger");
        System.out.println(record.topic());
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println("Logged in");
    }
}
