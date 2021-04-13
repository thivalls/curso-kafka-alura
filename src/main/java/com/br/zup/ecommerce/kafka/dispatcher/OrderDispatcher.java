package com.br.zup.ecommerce.kafka.dispatcher;

import com.br.zup.ecommerce.kafka.serializer.GsonSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class OrderDispatcher<T> implements Closeable {
    private final KafkaProducer producer;

    public OrderDispatcher() {
        this.producer = new KafkaProducer<String, T>(properties());
    }

    private static Callback getCallback() {
        return (RecordMetadata success, Exception err) -> {
            if (err != null) {
                System.out.println("eentrei aqui thiagaoooo ******************");
                err.printStackTrace();
                return;
            }

            System.out.println(success.topic() + ":::partition " + success.partition() + " :::offset " + success.offset() + " timestamp " + success.timestamp());
        };
    }

    // CONFIGURANDO KAFKA PARA CRIAR UM PRODUCER
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        return properties;
    }

    public void send(String topic, String key, T order) throws ExecutionException, InterruptedException {
        ProducerRecord record = new ProducerRecord(topic, key, order);
        this.producer.send(record, getCallback()).get();
    }

    @Override
    public void close() {
        producer.close();
    }
}
