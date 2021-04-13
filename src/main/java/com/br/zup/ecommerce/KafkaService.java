package com.br.zup.ecommerce;

import com.br.zup.ecommerce.kafka.serializer.GsonDeserializer;
import com.br.zup.ecommerce.kafka.serializer.GsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * RESPONSÁVEL POR CRIAR CONSUMIDORES(CONSUMERS)
 * Esta classe foi criada para tentar facilitar e generalizar a criação de consumidores para evitar
 * duplicação de código
 */

class KafkaService<T> implements Closeable {
    private KafkaConsumer<String, T> consumer = null;
    private ConsumerFunction parse = null;
    private Class<T> type;

    KafkaService(String group, String topic, ConsumerFunction parse, Class<T> type) {
        this.kafkaService(group, parse, type);
        consumer.subscribe(Collections.singletonList(topic));
        this.run();
    }

    KafkaService(String group, Pattern topic, ConsumerFunction parse, Class<T> type) {
        this.kafkaService(group, parse, type);
        consumer.subscribe(topic);
        this.run();
    }
    
    private void kafkaService(String groupId, ConsumerFunction parse, Class<T> type) {
        this.parse = parse;
        this.type = type;
        this.consumer = new KafkaConsumer<>(properties(type, groupId));
    }

    void run() {
        while (true) {
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Enviando email");
                for (var record : records) {
                    parse.consume(record);
                }
            }
        }
    }

    private Properties properties(Class<T> type, String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
