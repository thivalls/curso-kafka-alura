package com.br.zup.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction<T> {
    public void consume(ConsumerRecord<String, T> record);
}
