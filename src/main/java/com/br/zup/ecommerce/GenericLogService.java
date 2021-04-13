package com.br.zup.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

public class GenericLogService {
    public static void main(String[] args) {
        GenericLogService genericLogService = new GenericLogService();
        KafkaService kafkaService = new KafkaService(GenericLogService.class.getSimpleName(), Pattern.compile("ECOMMERCE.*"), genericLogService::parse);
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
