package com.br.zup.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // quero enviar uma mensagem para o kafka
        try (KafkaDispatcher kafkaDispatcher = new KafkaDispatcher()) {
            for (var i = 0; i < 10; i++) {
                String key = UUID.randomUUID().toString();
                String value = key + ", Thanks for you order. We are processing it.";
                kafkaDispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                String email = "my@email.com.br";
                kafkaDispatcher.send("ECOMMERCE_NEW_ORDER", key, email);
            }
        }
    }
}
