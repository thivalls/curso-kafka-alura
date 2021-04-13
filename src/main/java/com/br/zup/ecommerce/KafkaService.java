package com.br.zup.ecommerce;

import com.br.zup.ecommerce.kafka.serializer.GsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
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
    private Map<String, String> properties;

    KafkaService(String group, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this.kafkaService(group, parse, type, properties);
        consumer.subscribe(Collections.singletonList(topic));
        this.run();
    }

    KafkaService(String group, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this.kafkaService(group, parse, type, properties);
        consumer.subscribe(topic);
        this.run();
    }
    
    private void kafkaService(String groupId, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this.parse = parse;
        this.type = type;
        this.consumer = new KafkaConsumer<>(getProperties(type, groupId, properties));
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

    /**
     * FOI INSTALADO O GSON PARA QUE PUDESSEMOS SERIALIZAR E DESERIALIZAR OBJETOS PARA PASSAR PARA OS TOPICOS E CONSUMIR
     * NOS CONSUMIDORES, MAS EXISTEM CASOS QUE O VALOR PASSADO DEVERÁ SER REALMENTE UMA STRING, ENTÃO PASSAMOS MAIS UMA PARÂMETRO QUE
     * VAI RECEBER PROPRIEDADES PARA SOBRESCREVER(OVERWRITE) AS PROPRIEDADES PADRÕES.
     */
    private Properties getProperties(Class<T> type, String groupId, Map<String, String> overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
