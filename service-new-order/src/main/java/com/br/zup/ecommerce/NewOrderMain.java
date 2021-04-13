package com.br.zup.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // quero enviar uma mensagem para o kafka
        try (var orderKafkaDispatcher = new OrderDispatcher<Order>()) {
            try (var emailKafkaDispatcher = new OrderDispatcher<Email>()) {
                for (var i = 0; i < 10; i++) {
                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                    var order = new Order(userId, orderId, amount);
                    orderKafkaDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

                    Email email = new Email("New order created", "Thanks for your order");
                    emailKafkaDispatcher.send("ECOMMERCE_NEW_ORDER_EMAIL", userId, email);
                }
            }
        }
    }
}
