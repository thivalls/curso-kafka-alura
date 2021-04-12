package com.br.zup.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // quero enviar uma mensagem para o kafka
        var kafkaProducer = new KafkaProducer<String, String>(properties());

        String value = "order:1, user:2, price:122.00";
        String email = "Thanks for you order. We are processing it.";
        ProducerRecord<String, String> newOrderRecord = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
        ProducerRecord<String, String> emailRecord = new ProducerRecord<>("ECOMMERCE_NEW_ORDER_EMAIL", email, email);
        for(var i=100; i < 100; i++) {
            kafkaProducer.send(newOrderRecord, NewOrderMain::onCompletion).get();
            kafkaProducer.send(emailRecord, NewOrderMain::onCompletion).get();
        }
    }

    // CONFIGURANDO KAFKA PARA CRIAR UM PRODUCER
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static void onCompletion(RecordMetadata success, Exception err) {
        if (err != null) {
            err.printStackTrace();
            return;
        }

        System.out.println(success.topic() + ":::partition " + success.partition() + " :::offset " + success.offset() + " timestamp " + success.timestamp());
    }
}
