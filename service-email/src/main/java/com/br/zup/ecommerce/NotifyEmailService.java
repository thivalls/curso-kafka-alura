package com.br.zup.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class NotifyEmailService<T> {
    public static void main(String[] args) {
        var emailService = new NotifyEmailService();
        try (var service = new KafkaService<Email>(
                NotifyEmailService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER_EMAIL",
                emailService::parse,
                Email.class,
                Map.of()
        )) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, T> record) {
        System.out.println("---------------------------------------------");
        System.out.println("Sending email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        System.out.println("Email has been sent");
    }
}
