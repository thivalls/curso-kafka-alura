package com.br.zup.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class NotifyEmailService {
    public static void main(String[] args) {
        NotifyEmailService emailService = new NotifyEmailService();
        try (var service = new KafkaService(
                NotifyEmailService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER_EMAIL",
                emailService::parse,
                String.class,
                Map.of()
        )
        ) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
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
