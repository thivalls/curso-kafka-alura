package com.br.zup.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction {
    public void consume(ConsumerRecord<String, String> record);
}
